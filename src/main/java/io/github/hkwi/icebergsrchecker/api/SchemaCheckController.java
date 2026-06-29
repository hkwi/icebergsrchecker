package io.github.hkwi.icebergsrchecker.api;

import io.github.hkwi.icebergsrchecker.service.SchemaCheckerService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class SchemaCheckController {
    private final SchemaCheckerService checkerService;

    public SchemaCheckController(SchemaCheckerService checkerService) {
        this.checkerService = checkerService;
    }

    @PostMapping("/check")
    public ResponseEntity<SchemaCheckResponse> check(@RequestBody SchemaCheckRequest request) {
        if (request == null || isBlank(request.format()) || isBlank(request.schema())) {
            SchemaCheckResponse response = new SchemaCheckResponse(
                    false,
                    request == null ? null : request.format(),
                    0,
                    List.of(new SchemaIssue("request", "format and schema are required.")),
                    List.of(),
                    null,
                    null,
                    null
            );
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(response);
        }
        return ResponseEntity.ok(checkerService.check(request));
    }

    @GetMapping("/health")
    public Map<String, String> health() {
        return Map.of("status", "ok");
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
