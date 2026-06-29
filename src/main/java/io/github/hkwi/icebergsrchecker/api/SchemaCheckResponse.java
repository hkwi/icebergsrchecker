package io.github.hkwi.icebergsrchecker.api;

import java.util.List;
import java.util.Map;

public record SchemaCheckResponse(
        boolean ok,
        String format,
        long elapsedMs,
        List<SchemaIssue> errors,
        List<SchemaIssue> warnings,
        Map<String, Object> connectSchema,
        Map<String, Object> icebergSchema,
        Map<String, Object> parquetDryRun
) {
}
