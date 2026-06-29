package io.github.hkwi.icebergsrchecker.service;

import io.github.hkwi.icebergsrchecker.api.SchemaCheckRequest;
import io.github.hkwi.icebergsrchecker.api.SchemaCheckResponse;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaCheckerServiceTest {
    private final SchemaCheckerService service = new SchemaCheckerService();

    @Test
    void avroRecordShouldPassWithoutSampleValue() {
        String schema = """
                {
                  "type": "record",
                  "name": "E",
                  "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "count", "type": "int"}
                  ]
                }
                """;

        SchemaCheckResponse response = service.check("avro", schema);

        assertTrue(response.ok(), response.errors().toString());
        assertNotNull(response.connectSchema());
        assertTrue((Boolean) response.parquetDryRun().get("ok"));
        assertFalse((Boolean) response.parquetDryRun().get("sampleValueUsed"));
    }

    @Test
    void avroJsonSampleShouldBeWrittenWhenProvided() {
        String schema = """
                {
                  "type": "record",
                  "name": "E",
                  "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "count", "type": "int"}
                  ]
                }
                """;

        SchemaCheckResponse response = service.check(new SchemaCheckRequest(
                "avro",
                schema,
                "{\"id\":\"a\",\"count\":1}",
                "avro-json",
                false,
                Map.of()
        ));

        assertTrue(response.ok(), response.errors().toString());
        assertTrue((Boolean) response.parquetDryRun().get("sampleValueUsed"));
    }

    @Test
    void jsonSchemaObjectShouldPassWithoutSampleValue() {
        String schema = """
                {
                  "type": "object",
                  "required": ["id"],
                  "properties": {
                    "id": {"type": "string"},
                    "tags": {"type": "array", "items": {"type": "string"}}
                  }
                }
                """;

        SchemaCheckResponse response = service.check("json-schema", schema);

        assertTrue(response.ok(), response.errors().toString());
        assertTrue((Boolean) response.parquetDryRun().get("ok"));
    }

    @Test
    void emptyJsonSchemaObjectShouldFailParquetDryRun() {
        String schema = """
                {
                  "type": "object",
                  "properties": {}
                }
                """;

        SchemaCheckResponse response = service.check("json-schema", schema);

        assertFalse(response.ok());
    }

    @Test
    void protobufSimpleMessageShouldPassWithoutSampleValue() {
        String schema = """
                syntax = "proto3";
                message E {
                  string id = 1;
                  repeated int64 values = 2;
                }
                """;

        SchemaCheckResponse response = service.check("protobuf", schema);

        assertTrue(response.ok(), response.errors().toString());
        assertTrue((Boolean) response.parquetDryRun().get("ok"));
        assertFalse((Boolean) response.parquetDryRun().get("sampleValueUsed"));
    }

    @Test
    void protobufJsonSampleShouldBeWrittenWhenProvided() {
        String schema = """
                syntax = "proto3";
                message E {
                  string id = 1;
                  int64 count = 2;
                }
                """;

        SchemaCheckResponse response = service.check(new SchemaCheckRequest(
                "protobuf",
                schema,
                "{\"id\":\"a\",\"count\":\"1\"}",
                "protobuf-json",
                false,
                Map.of()
        ));

        assertTrue(response.ok(), response.errors().toString());
        assertTrue((Boolean) response.parquetDryRun().get("sampleValueUsed"));
    }

    @Test
    void protobufTextSampleShouldBeWrittenWhenProvided() {
        String schema = """
                syntax = "proto3";
                message E {
                  string id = 1;
                  int64 count = 2;
                }
                """;

        SchemaCheckResponse response = service.check(new SchemaCheckRequest(
                "protobuf",
                schema,
                "id: \"a\"\ncount: 1",
                "protobuf-text",
                false,
                Map.of()
        ));

        assertTrue(response.ok(), response.errors().toString());
        assertTrue((Boolean) response.parquetDryRun().get("sampleValueUsed"));
    }
}
