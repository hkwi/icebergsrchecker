package io.github.hkwi.icebergsrchecker.service;

import io.github.hkwi.icebergsrchecker.api.SchemaCheckResponse;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaCheckerServiceTest {
    private final SchemaCheckerService service = new SchemaCheckerService();

    @Test
    void avroRecordShouldPass() {
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
        assertTrue(response.ok());
    }

    @Test
    void jsonSchemaCombinatorShouldFail() {
        String schema = """
                {
                  "type": "object",
                  "properties": {
                    "x": {
                      "oneOf": [{"type": "string"}, {"type": "number"}]
                    }
                  }
                }
                """;
        SchemaCheckResponse response = service.check("json-schema", schema);
        assertFalse(response.ok());
    }

    @Test
    void protobufSimpleMessageShouldPass() {
        String schema = """
                syntax = "proto3";
                message E {
                  string id = 1;
                  repeated int64 values = 2;
                }
                """;
        SchemaCheckResponse response = service.check("protobuf", schema);
        assertTrue(response.ok());
    }
}
