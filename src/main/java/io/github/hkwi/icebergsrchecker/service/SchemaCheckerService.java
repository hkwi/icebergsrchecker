package io.github.hkwi.icebergsrchecker.service;

import io.github.hkwi.icebergsrchecker.api.SchemaCheckResponse;
import io.github.hkwi.icebergsrchecker.api.SchemaIssue;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Service
public class SchemaCheckerService {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public SchemaCheckResponse check(String format, String schemaText) {
        long start = System.currentTimeMillis();
        CheckContext ctx = new CheckContext();
        Schema icebergSchema = null;

        try {
            String normalized = normalizeFormat(format);
            if ("avro".equals(normalized)) {
                icebergSchema = checkAvro(schemaText, ctx);
            } else if ("json-schema".equals(normalized)) {
                icebergSchema = checkJsonSchema(schemaText, ctx);
            } else if ("protobuf".equals(normalized)) {
                icebergSchema = checkProtobuf(schemaText, ctx);
            } else {
                ctx.error("root", "Unsupported format: " + format);
            }

            return new SchemaCheckResponse(
                    ctx.errors.isEmpty(),
                    normalized,
                    System.currentTimeMillis() - start,
                    List.copyOf(ctx.errors),
                    List.copyOf(ctx.warnings),
                    icebergSchema == null ? null : schemaToMap(icebergSchema)
            );
        } catch (Exception ex) {
            ctx.error("root", "Parse error: " + ex.getMessage());
            return new SchemaCheckResponse(
                    false,
                    normalizeFormat(format),
                    System.currentTimeMillis() - start,
                    List.copyOf(ctx.errors),
                    List.copyOf(ctx.warnings),
                    null
            );
        }
    }

    private Schema checkAvro(String schemaText, CheckContext ctx) {
        AvroSchema avroSchema = new AvroSchema(schemaText);
        org.apache.avro.Schema rawSchema = avroSchema.rawSchema();
        if (rawSchema.getType() != org.apache.avro.Schema.Type.RECORD) {
            ctx.error("root", "Top-level Avro schema must be a record.");
            return null;
        }
        return AvroSchemaUtil.toIceberg(rawSchema);
    }

    private Schema checkJsonSchema(String schemaText, CheckContext ctx) throws JsonProcessingException {
        new JsonSchema(schemaText);
        JsonNode root = MAPPER.readTree(schemaText);
        if (!"object".equals(typeName(root))) {
            ctx.error("root", "Top-level JSON Schema should be object for row mapping.");
            return null;
        }
        List<Types.NestedField> fields = mapJsonObjectFields(root, "root", ctx);
        return new Schema(fields);
    }

    private List<Types.NestedField> mapJsonObjectFields(JsonNode objectNode, String path, CheckContext ctx) {
        JsonNode properties = objectNode.path("properties");
        Set<String> required = new HashSet<>();
        JsonNode requiredNode = objectNode.path("required");
        if (requiredNode.isArray()) {
            requiredNode.forEach(n -> required.add(n.asText()));
        }

        if (objectNode.has("oneOf") || objectNode.has("anyOf") || objectNode.has("allOf") || objectNode.has("$ref")) {
            ctx.error(path, "JSON Schema combinators and $ref are not supported.");
        }

        List<Types.NestedField> fields = new ArrayList<>();
        if (!properties.isObject()) {
            return fields;
        }
        properties.fields().forEachRemaining(entry -> {
            String fieldName = entry.getKey();
            JsonNode node = entry.getValue();
            String fieldPath = path + "." + fieldName;
            Type fieldType = mapJsonType(node, fieldPath, ctx);
            boolean isRequired = required.contains(fieldName);
            Types.NestedField field = isRequired
                    ? Types.NestedField.required(ctx.nextId(), fieldName, fieldType)
                    : Types.NestedField.optional(ctx.nextId(), fieldName, fieldType);
            fields.add(field);
        });
        return fields;
    }

    private Type mapJsonType(JsonNode node, String path, CheckContext ctx) {
        String type = typeName(node);
        if (type == null) {
            ctx.error(path, "Unable to resolve JSON Schema type.");
            return Types.StringType.get();
        }

        if ("string".equals(type)) {
            return Types.StringType.get();
        }
        if ("boolean".equals(type)) {
            return Types.BooleanType.get();
        }
        if ("integer".equals(type)) {
            return Types.LongType.get();
        }
        if ("number".equals(type)) {
            return Types.DoubleType.get();
        }
        if ("array".equals(type)) {
            JsonNode item = node.path("items");
            Type elementType = mapJsonType(item, path + "[]", ctx);
            return Types.ListType.ofOptional(ctx.nextId(), elementType);
        }
        if ("object".equals(type)) {
            if (node.has("patternProperties")) {
                ctx.error(path, "patternProperties is not supported.");
            }
            List<Types.NestedField> fields = mapJsonObjectFields(node, path, ctx);
            return Types.StructType.of(fields);
        }

        ctx.error(path, "Unsupported JSON Schema type: " + type);
        return Types.StringType.get();
    }

    private String typeName(JsonNode node) {
        JsonNode type = node.path("type");
        if (type.isTextual()) {
            return type.asText();
        }
        if (type.isArray()) {
            for (JsonNode part : type) {
                if (!"null".equals(part.asText())) {
                    return part.asText();
                }
            }
        }
        return null;
    }

    private Schema checkProtobuf(String schemaText, CheckContext ctx) {
        ProtobufSchema protobufSchema = new ProtobufSchema(schemaText);
        com.google.protobuf.Descriptors.Descriptor descriptor;
        try {
            descriptor = protobufSchema.toDescriptor();
        } catch (Exception ex) {
            ctx.error("root", "Unable to parse protobuf descriptor: " + ex.getMessage());
            return null;
        }
        Types.StructType rootType = mapDescriptor(descriptor, "root." + descriptor.getName(), ctx, new HashSet<>());
        return new Schema(rootType.fields());
    }

    private Types.StructType mapDescriptor(
            com.google.protobuf.Descriptors.Descriptor descriptor,
            String path,
            CheckContext ctx,
            Set<String> seen
    ) {
        if (!seen.add(descriptor.getFullName())) {
            ctx.error(path, "Recursive protobuf type is not supported: " + descriptor.getFullName());
            return Types.StructType.of(List.of());
        }

        List<Types.NestedField> fields = new ArrayList<>();
        for (com.google.protobuf.Descriptors.FieldDescriptor field : descriptor.getFields()) {
            String fieldPath = path + "." + field.getName();
            Type type = mapProtoFieldType(field, fieldPath, ctx, new HashSet<>(seen));
            fields.add(Types.NestedField.optional(ctx.nextId(), field.getName(), type));
        }

        return Types.StructType.of(fields);
    }

    private Type mapProtoFieldType(
            com.google.protobuf.Descriptors.FieldDescriptor field,
            String path,
            CheckContext ctx,
            Set<String> seen
    ) {
        if (field.isMapField()) {
            com.google.protobuf.Descriptors.FieldDescriptor valueField = field.getMessageType().findFieldByName("value");
            if (valueField == null) {
                ctx.error(path, "Unsupported protobuf map type.");
                return Types.MapType.ofOptional(ctx.nextId(), ctx.nextId(), Types.StringType.get(), Types.StringType.get());
            }
            Type valueType = scalarProtoType(valueField, path + "{}", ctx, seen);
            return Types.MapType.ofOptional(ctx.nextId(), ctx.nextId(), Types.StringType.get(), valueType);
        }

        Type scalar = scalarProtoType(field, path, ctx, seen);
        if (field.isRepeated()) {
            return Types.ListType.ofOptional(ctx.nextId(), scalar);
        }
        return scalar;
    }

    private Type scalarProtoType(
            com.google.protobuf.Descriptors.FieldDescriptor field,
            String path,
            CheckContext ctx,
            Set<String> seen
    ) {
        return switch (field.getType()) {
            case STRING -> Types.StringType.get();
            case BOOL -> Types.BooleanType.get();
            case BYTES -> Types.BinaryType.get();
            case DOUBLE -> Types.DoubleType.get();
            case FLOAT -> Types.FloatType.get();
            case INT32, SINT32, SFIXED32 -> Types.IntegerType.get();
            case INT64, SINT64, SFIXED64 -> Types.LongType.get();
            case UINT32, FIXED32, UINT64, FIXED64 -> {
                ctx.warning(path, "Unsigned/fixed protobuf integer is mapped to Iceberg long.");
                yield Types.LongType.get();
            }
            case ENUM -> {
                ctx.warning(path, "Enum is mapped to Iceberg string.");
                yield Types.StringType.get();
            }
            default -> {
                if (field.getType() == com.google.protobuf.Descriptors.FieldDescriptor.Type.MESSAGE) {
                    yield mapDescriptor(field.getMessageType(), path, ctx, seen);
                }
                ctx.error(path, "Unsupported protobuf type: " + field.getType());
                yield Types.StringType.get();
            }
        };
    }

    private String normalizeFormat(String format) {
        return format == null ? "" : format.trim().toLowerCase();
    }

    private Map<String, Object> schemaToMap(Schema schema) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("schemaId", schema.schemaId());
        result.put("fields", fieldsToMap(schema.columns()));
        return result;
    }

    private List<Map<String, Object>> fieldsToMap(List<Types.NestedField> fields) {
        List<Map<String, Object>> mapped = new ArrayList<>();
        for (Types.NestedField field : fields) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("id", field.fieldId());
            item.put("name", field.name());
            item.put("required", field.isRequired());
            item.put("type", typeToMap(field.type()));
            mapped.add(item);
        }
        return mapped;
    }

    private Object typeToMap(Type type) {
        if (type.isPrimitiveType()) {
            return type.toString();
        }
        if (type.isStructType()) {
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("type", "struct");
            result.put("fields", fieldsToMap(type.asStructType().fields()));
            return result;
        }
        if (type.isListType()) {
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("type", "list");
            result.put("elementId", type.asListType().elementId());
            result.put("elementRequired", type.asListType().isElementRequired());
            result.put("element", typeToMap(type.asListType().elementType()));
            return result;
        }
        if (type.isMapType()) {
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("type", "map");
            result.put("keyId", type.asMapType().keyId());
            result.put("valueId", type.asMapType().valueId());
            result.put("valueRequired", type.asMapType().isValueRequired());
            result.put("key", typeToMap(type.asMapType().keyType()));
            result.put("value", typeToMap(type.asMapType().valueType()));
            return result;
        }
        return type.toString();
    }

    private static final class CheckContext {
        private int nextId = 1;
        private final List<SchemaIssue> errors = new ArrayList<>();
        private final List<SchemaIssue> warnings = new ArrayList<>();

        int nextId() {
            return nextId++;
        }

        void error(String path, String message) {
            errors.add(new SchemaIssue(path, message));
        }

        void warning(String path, String message) {
            warnings.add(new SchemaIssue(path, message));
        }
    }

}
