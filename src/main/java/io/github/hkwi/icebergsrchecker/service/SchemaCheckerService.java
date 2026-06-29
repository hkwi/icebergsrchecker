package io.github.hkwi.icebergsrchecker.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.TextFormat;
import com.google.protobuf.util.JsonFormat;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import io.confluent.connect.json.JsonSchemaData;
import io.confluent.connect.json.JsonSchemaDataConfig;
import io.confluent.connect.protobuf.ProtobufData;
import io.confluent.connect.protobuf.ProtobufDataConfig;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;
import io.github.hkwi.icebergsrchecker.api.SchemaCheckRequest;
import io.github.hkwi.icebergsrchecker.api.SchemaCheckResponse;
import io.github.hkwi.icebergsrchecker.api.SchemaIssue;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.springframework.stereotype.Service;

@Service
public class SchemaCheckerService {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String VALUE_CONVERTER_PREFIX = "value.converter.";

    public SchemaCheckResponse check(String format, String schemaText) {
        return check(new SchemaCheckRequest(format, schemaText, null, null, false, Map.of()));
    }

    public SchemaCheckResponse check(SchemaCheckRequest request) {
        long start = System.nanoTime();
        CheckContext ctx = new CheckContext();
        String normalized = normalizeFormat(request.format());
        ConnectInput connectInput = null;
        Schema icebergSchema = null;
        Map<String, Object> parquetDryRun = null;

        try {
            connectInput = toConnectInput(normalized, request, ctx);
            if (connectInput != null && ctx.errors.isEmpty()) {
                Type rootType = toIcebergType(
                        connectInput.schema(),
                        ctx,
                        Boolean.TRUE.equals(request.schemaForceOptional())
                );
                Types.StructType rootStruct = rootType.asStructType();
                icebergSchema = new Schema(rootStruct.fields());

                Record record = connectInput.value() == null
                        ? dummyRecord(rootStruct)
                        : toRecord(rootStruct, connectInput.value());
                parquetDryRun = runParquetDryRun(icebergSchema, record, connectInput.value() != null, ctx);
            }
        } catch (Exception ex) {
            ctx.error("root", dryRunError(ex));
        }

        return new SchemaCheckResponse(
                ctx.errors.isEmpty(),
                normalized,
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start),
                List.copyOf(ctx.errors),
                List.copyOf(ctx.warnings),
                connectInput == null ? null : connectSchemaToMap(connectInput.schema()),
                icebergSchema == null ? null : schemaToMap(icebergSchema),
                parquetDryRun
        );
    }

    private ConnectInput toConnectInput(String format, SchemaCheckRequest request, CheckContext ctx)
            throws Exception {
        Map<String, Object> converterConfig = converterConfig(request.converterConfig());
        boolean hasSample = !isBlank(request.sampleValue());

        return switch (format) {
            case "avro" -> avroToConnect(
                    request.schema(), request.sampleValue(), sampleFormat(request, "avro-json"), hasSample,
                    converterConfig);
            case "json-schema" -> jsonSchemaToConnect(
                    request.schema(), request.sampleValue(), sampleFormat(request, "json"), hasSample,
                    converterConfig);
            case "protobuf" -> protobufToConnect(
                    request.schema(), request.sampleValue(), sampleFormat(request, "protobuf-json"), hasSample,
                    converterConfig);
            default -> {
                ctx.error("root", "Unsupported format: " + request.format());
                yield null;
            }
        };
    }

    private ConnectInput avroToConnect(
            String schemaText,
            String sampleValue,
            String sampleFormat,
            boolean hasSample,
            Map<String, Object> converterConfig
    ) throws IOException {
        AvroSchema avroSchema = new AvroSchema(schemaText);
        AvroData avroData = new AvroData(new AvroDataConfig(converterConfig));
        org.apache.kafka.connect.data.Schema connectSchema = avroData.toConnectSchema(avroSchema.rawSchema());
        Object connectValue = null;
        if (hasSample) {
            if (!"avro-json".equals(sampleFormat)) {
                throw new IllegalArgumentException("Avro sampleFormat must be avro-json.");
            }
            Object avroValue = parseAvroJson(avroSchema.rawSchema(), sampleValue);
            SchemaAndValue schemaAndValue = avroData.toConnectData(avroSchema.rawSchema(), avroValue);
            connectValue = schemaAndValue.value();
        }
        return new ConnectInput(connectSchema, connectValue);
    }

    private ConnectInput jsonSchemaToConnect(
            String schemaText,
            String sampleValue,
            String sampleFormat,
            boolean hasSample,
            Map<String, Object> converterConfig
    ) throws JsonProcessingException {
        JsonSchema jsonSchema = new JsonSchema(schemaText);
        JsonSchemaData jsonSchemaData = new JsonSchemaData(new JsonSchemaDataConfig(converterConfig));
        org.apache.kafka.connect.data.Schema connectSchema = jsonSchemaData.toConnectSchema(jsonSchema);
        Object connectValue = null;
        if (hasSample) {
            if (!"json".equals(sampleFormat)) {
                throw new IllegalArgumentException("JSON Schema sampleFormat must be json.");
            }
            JsonNode jsonValue = MAPPER.readTree(sampleValue);
            connectValue = jsonSchemaData.toConnectData(connectSchema, jsonValue);
        }
        return new ConnectInput(connectSchema, connectValue);
    }

    private ConnectInput protobufToConnect(
            String schemaText,
            String sampleValue,
            String sampleFormat,
            boolean hasSample,
            Map<String, Object> converterConfig
    ) throws Exception {
        ProtobufSchema protobufSchema = new ProtobufSchema(schemaText);
        ProtobufData protobufData = new ProtobufData(new ProtobufDataConfig(converterConfig));
        org.apache.kafka.connect.data.Schema connectSchema = protobufData.toConnectSchema(protobufSchema);
        Object connectValue = null;
        if (hasSample) {
            DynamicMessage.Builder builder = DynamicMessage.newBuilder(protobufSchema.toDescriptor());
            if ("protobuf-json".equals(sampleFormat)) {
                JsonFormat.parser().merge(sampleValue, builder);
            } else if ("protobuf-text".equals(sampleFormat)) {
                TextFormat.getParser().merge(sampleValue, builder);
            } else {
                throw new IllegalArgumentException(
                        "Protobuf sampleFormat must be protobuf-json or protobuf-text.");
            }
            SchemaAndValue schemaAndValue = protobufData.toConnectData(protobufSchema, builder.build());
            connectValue = schemaAndValue.value();
        }
        return new ConnectInput(connectSchema, connectValue);
    }

    private Object parseAvroJson(org.apache.avro.Schema schema, String json) throws IOException {
        GenericDatumReader<Object> reader = new GenericDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().jsonDecoder(schema, json);
        return reader.read(null, decoder);
    }

    private Type toIcebergType(
            org.apache.kafka.connect.data.Schema valueSchema,
            CheckContext ctx,
            boolean schemaForceOptional
    ) {
        return switch (valueSchema.type()) {
            case BOOLEAN -> Types.BooleanType.get();
            case BYTES -> {
                if (Decimal.LOGICAL_NAME.equals(valueSchema.name())) {
                    int scale = Integer.parseInt(valueSchema.parameters().get(Decimal.SCALE_FIELD));
                    yield Types.DecimalType.of(38, scale);
                }
                yield Types.BinaryType.get();
            }
            case INT8, INT16 -> Types.IntegerType.get();
            case INT32 -> {
                if (org.apache.kafka.connect.data.Date.LOGICAL_NAME.equals(valueSchema.name())) {
                    yield Types.DateType.get();
                } else if (org.apache.kafka.connect.data.Time.LOGICAL_NAME.equals(valueSchema.name())) {
                    yield Types.TimeType.get();
                }
                yield Types.IntegerType.get();
            }
            case INT64 -> {
                if (org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME.equals(valueSchema.name())) {
                    yield Types.TimestampType.withZone();
                }
                yield Types.LongType.get();
            }
            case FLOAT32 -> Types.FloatType.get();
            case FLOAT64 -> Types.DoubleType.get();
            case ARRAY -> {
                Type elementType = toIcebergType(valueSchema.valueSchema(), ctx, schemaForceOptional);
                boolean optional = schemaForceOptional || valueSchema.valueSchema().isOptional();
                yield optional
                        ? Types.ListType.ofOptional(ctx.nextId(), elementType)
                        : Types.ListType.ofRequired(ctx.nextId(), elementType);
            }
            case MAP -> {
                Type keyType = toIcebergType(valueSchema.keySchema(), ctx, schemaForceOptional);
                Type valueType = toIcebergType(valueSchema.valueSchema(), ctx, schemaForceOptional);
                boolean optional = schemaForceOptional || valueSchema.valueSchema().isOptional();
                yield optional
                        ? Types.MapType.ofOptional(ctx.nextId(), ctx.nextId(), keyType, valueType)
                        : Types.MapType.ofRequired(ctx.nextId(), ctx.nextId(), keyType, valueType);
            }
            case STRUCT -> {
                List<Types.NestedField> structFields = valueSchema.fields().stream()
                        .map(field -> Types.NestedField.builder()
                                .isOptional(schemaForceOptional || field.schema().isOptional())
                                .withId(ctx.nextId())
                                .ofType(toIcebergType(field.schema(), ctx, schemaForceOptional))
                                .withName(field.name())
                                .build())
                        .collect(Collectors.toList());
                yield Types.StructType.of(structFields);
            }
            case STRING -> {
                if ("uuid".equals(valueSchema.name())) {
                    yield Types.UUIDType.get();
                }
                yield Types.StringType.get();
            }
        };
    }

    private Map<String, Object> runParquetDryRun(
            Schema icebergSchema,
            Record record,
            boolean sampleValueUsed,
            CheckContext ctx
    ) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("sampleValueUsed", sampleValueUsed);
        result.put("recordSource", sampleValueUsed ? "sampleValue" : "synthetic");

        Path file = null;
        try {
            file = java.nio.file.Files.createTempFile("iceberg-sr-checker-", ".parquet");
            java.nio.file.Files.deleteIfExists(file);
            DataWriter<Record> writer = Parquet.writeData(org.apache.iceberg.Files.localOutput(file.toFile()))
                    .schema(icebergSchema)
                    .createWriterFunc(GenericParquetWriter::create)
                    .overwrite()
                    .withSpec(PartitionSpec.unpartitioned())
                    .build();
            try {
                writer.write(record);
            } finally {
                writer.close();
            }
            result.put("format", FileFormat.PARQUET.name());
            result.put("recordCount", 1);
            result.put("fileBytes", writer.toDataFile().fileSizeInBytes());
            result.put("ok", true);
        } catch (Exception ex) {
            ctx.error("parquet", dryRunError(ex));
            result.put("ok", false);
            result.put("error", dryRunError(ex));
        } finally {
            if (file != null) {
                try {
                    java.nio.file.Files.deleteIfExists(file);
                } catch (IOException ignored) {
                    // best effort cleanup
                }
            }
        }
        return result;
    }

    private Record toRecord(Types.StructType struct, Object value) {
        if (value instanceof Struct connectStruct) {
            GenericRecord record = GenericRecord.create(struct);
            for (Types.NestedField field : struct.fields()) {
                record.setField(field.name(), toIcebergValue(field.type(), connectStruct.get(field.name())));
            }
            return record;
        }
        if (value instanceof Map<?, ?> map) {
            GenericRecord record = GenericRecord.create(struct);
            for (Types.NestedField field : struct.fields()) {
                record.setField(field.name(), toIcebergValue(field.type(), map.get(field.name())));
            }
            return record;
        }
        throw new IllegalArgumentException(
                "Top-level sample value must be converted to Connect Struct or Map, but was "
                        + value.getClass().getName());
    }

    private Object toIcebergValue(Type type, Object value) {
        if (value == null) {
            return null;
        }
        if (type.isStructType()) {
            return toRecord(type.asStructType(), value);
        }
        if (type.isListType()) {
            Type elementType = type.asListType().elementType();
            if (value instanceof Collection<?> collection) {
                return collection.stream()
                        .map(item -> toIcebergValue(elementType, item))
                        .collect(Collectors.toList());
            }
            throw new IllegalArgumentException("Expected collection for list value, but was " + value.getClass());
        }
        if (type.isMapType()) {
            Type keyType = type.asMapType().keyType();
            Type valueType = type.asMapType().valueType();
            if (value instanceof Map<?, ?> map) {
                Map<Object, Object> converted = new LinkedHashMap<>();
                map.forEach((k, v) -> converted.put(toIcebergValue(keyType, k), toIcebergValue(valueType, v)));
                return converted;
            }
            throw new IllegalArgumentException("Expected map value, but was " + value.getClass());
        }
        return toPrimitiveValue(type.asPrimitiveType(), value);
    }

    private Object toPrimitiveValue(Type.PrimitiveType type, Object value) {
        return switch (type.typeId()) {
            case BOOLEAN -> (Boolean) value;
            case INTEGER -> value instanceof Number number ? number.intValue() : Integer.valueOf(value.toString());
            case LONG -> value instanceof Number number ? number.longValue() : Long.valueOf(value.toString());
            case FLOAT -> value instanceof Number number ? number.floatValue() : Float.valueOf(value.toString());
            case DOUBLE -> value instanceof Number number ? number.doubleValue() : Double.valueOf(value.toString());
            case DATE -> toLocalDate(value);
            case TIME -> toLocalTime(value);
            case TIMESTAMP -> toTimestampValue((Types.TimestampType) type, value);
            case STRING -> value.toString();
            case UUID -> value instanceof UUID uuid ? uuid : UUID.fromString(value.toString());
            case BINARY -> toByteBuffer(value);
            case FIXED -> value instanceof byte[] bytes ? bytes : toByteBuffer(value).array();
            case DECIMAL -> value instanceof BigDecimal decimal ? decimal : new BigDecimal(value.toString());
            default -> value;
        };
    }

    private LocalDate toLocalDate(Object value) {
        if (value instanceof LocalDate localDate) {
            return localDate;
        }
        if (value instanceof java.util.Date date) {
            return Instant.ofEpochMilli(date.getTime()).atZone(ZoneOffset.UTC).toLocalDate();
        }
        if (value instanceof Number number) {
            return LocalDate.ofEpochDay(number.longValue());
        }
        return LocalDate.parse(value.toString());
    }

    private LocalTime toLocalTime(Object value) {
        if (value instanceof LocalTime localTime) {
            return localTime;
        }
        if (value instanceof java.util.Date date) {
            return Instant.ofEpochMilli(date.getTime()).atZone(ZoneOffset.UTC).toLocalTime();
        }
        if (value instanceof Number number) {
            return LocalTime.ofNanoOfDay(number.longValue() * 1000L);
        }
        return LocalTime.parse(value.toString());
    }

    private Object toTimestampValue(Types.TimestampType type, Object value) {
        if (type.shouldAdjustToUTC()) {
            if (value instanceof OffsetDateTime offsetDateTime) {
                return offsetDateTime;
            }
            if (value instanceof java.util.Date date) {
                return Instant.ofEpochMilli(date.getTime()).atOffset(ZoneOffset.UTC);
            }
            if (value instanceof Number number) {
                return Instant.ofEpochMilli(number.longValue()).atOffset(ZoneOffset.UTC);
            }
            return OffsetDateTime.parse(value.toString());
        }

        if (value instanceof LocalDateTime localDateTime) {
            return localDateTime;
        }
        if (value instanceof java.util.Date date) {
            return Instant.ofEpochMilli(date.getTime()).atZone(ZoneOffset.UTC).toLocalDateTime();
        }
        if (value instanceof Number number) {
            return Instant.ofEpochMilli(number.longValue()).atZone(ZoneOffset.UTC).toLocalDateTime();
        }
        return LocalDateTime.parse(value.toString());
    }

    private ByteBuffer toByteBuffer(Object value) {
        if (value instanceof ByteBuffer byteBuffer) {
            return byteBuffer;
        }
        if (value instanceof byte[] bytes) {
            return ByteBuffer.wrap(bytes);
        }
        return ByteBuffer.wrap(value.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    private Record dummyRecord(Types.StructType struct) {
        GenericRecord record = GenericRecord.create(struct);
        for (Types.NestedField field : struct.fields()) {
            record.setField(field.name(), dummyValue(field.type()));
        }
        return record;
    }

    private Object dummyValue(Type type) {
        if (type.isStructType()) {
            return dummyRecord(type.asStructType());
        }
        if (type.isListType()) {
            return List.of();
        }
        if (type.isMapType()) {
            return Map.of();
        }

        return switch (type.typeId()) {
            case BOOLEAN -> false;
            case INTEGER -> 0;
            case LONG -> 0L;
            case FLOAT -> 0.0F;
            case DOUBLE -> 0.0D;
            case DATE -> LocalDate.ofEpochDay(0);
            case TIME -> LocalTime.MIDNIGHT;
            case TIMESTAMP -> {
                Types.TimestampType timestampType = (Types.TimestampType) type.asPrimitiveType();
                yield timestampType.shouldAdjustToUTC()
                        ? OffsetDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC)
                        : LocalDateTime.ofInstant(Instant.EPOCH, ZoneOffset.UTC);
            }
            case STRING -> "";
            case UUID -> new UUID(0L, 0L);
            case BINARY -> ByteBuffer.wrap(new byte[0]);
            case FIXED -> new byte[((Types.FixedType) type.asPrimitiveType()).length()];
            case DECIMAL -> BigDecimal.ZERO.setScale(((Types.DecimalType) type.asPrimitiveType()).scale());
            default -> null;
        };
    }

    private Map<String, Object> converterConfig(Map<String, Object> requestConfig) {
        if (requestConfig == null || requestConfig.isEmpty()) {
            return Map.of();
        }

        Map<String, Object> normalized = new LinkedHashMap<>();
        requestConfig.forEach((key, value) -> {
            if (key != null && key.startsWith(VALUE_CONVERTER_PREFIX)) {
                normalized.put(key.substring(VALUE_CONVERTER_PREFIX.length()), value);
            } else if (key != null) {
                normalized.put(key, value);
            }
        });
        return normalized;
    }

    private String sampleFormat(SchemaCheckRequest request, String defaultFormat) {
        if (isBlank(request.sampleFormat()) || "auto".equalsIgnoreCase(request.sampleFormat())) {
            return defaultFormat;
        }
        return request.sampleFormat().trim().toLowerCase();
    }

    private String normalizeFormat(String format) {
        return format == null ? "" : format.trim().toLowerCase();
    }

    private String dryRunError(Exception ex) {
        String message = ex.getMessage();
        return ex.getClass().getSimpleName() + (message == null ? "" : ": " + message);
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private Map<String, Object> schemaToMap(Schema schema) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("schemaId", schema.schemaId());
        result.put("fields", icebergFieldsToMap(schema.columns()));
        return result;
    }

    private List<Map<String, Object>> icebergFieldsToMap(List<Types.NestedField> fields) {
        List<Map<String, Object>> mapped = new ArrayList<>();
        for (Types.NestedField field : fields) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("id", field.fieldId());
            item.put("name", field.name());
            item.put("required", field.isRequired());
            item.put("type", icebergTypeToMap(field.type()));
            mapped.add(item);
        }
        return mapped;
    }

    private Object icebergTypeToMap(Type type) {
        if (type.isPrimitiveType()) {
            return type.toString();
        }
        if (type.isStructType()) {
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("type", "struct");
            result.put("fields", icebergFieldsToMap(type.asStructType().fields()));
            return result;
        }
        if (type.isListType()) {
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("type", "list");
            result.put("elementId", type.asListType().elementId());
            result.put("elementRequired", type.asListType().isElementRequired());
            result.put("element", icebergTypeToMap(type.asListType().elementType()));
            return result;
        }
        if (type.isMapType()) {
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("type", "map");
            result.put("keyId", type.asMapType().keyId());
            result.put("valueId", type.asMapType().valueId());
            result.put("valueRequired", type.asMapType().isValueRequired());
            result.put("key", icebergTypeToMap(type.asMapType().keyType()));
            result.put("value", icebergTypeToMap(type.asMapType().valueType()));
            return result;
        }
        return type.toString();
    }

    private Map<String, Object> connectSchemaToMap(org.apache.kafka.connect.data.Schema schema) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("type", schema.type().name());
        result.put("optional", schema.isOptional());
        if (schema.name() != null) {
            result.put("name", schema.name());
        }
        if (schema.version() != null) {
            result.put("version", schema.version());
        }
        if (schema.parameters() != null && !schema.parameters().isEmpty()) {
            result.put("parameters", schema.parameters());
        }
        if (schema.type() == org.apache.kafka.connect.data.Schema.Type.STRUCT) {
            result.put("fields", schema.fields().stream()
                    .map(field -> {
                        Map<String, Object> item = new LinkedHashMap<>();
                        item.put("name", field.name());
                        item.put("index", field.index());
                        item.put("schema", connectSchemaToMap(field.schema()));
                        return item;
                    })
                    .collect(Collectors.toList()));
        } else if (schema.type() == org.apache.kafka.connect.data.Schema.Type.ARRAY) {
            result.put("valueSchema", connectSchemaToMap(schema.valueSchema()));
        } else if (schema.type() == org.apache.kafka.connect.data.Schema.Type.MAP) {
            result.put("keySchema", connectSchemaToMap(schema.keySchema()));
            result.put("valueSchema", connectSchemaToMap(schema.valueSchema()));
        }
        return result;
    }

    private record ConnectInput(org.apache.kafka.connect.data.Schema schema, Object value) {
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
