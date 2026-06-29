package io.github.hkwi.icebergsrchecker.api;

import java.util.Map;

public record SchemaCheckRequest(
        String format,
        String schema,
        String sampleValue,
        String sampleFormat,
        Boolean schemaForceOptional,
        Map<String, Object> converterConfig
) {
}
