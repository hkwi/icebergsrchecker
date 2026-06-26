# Iceberg Schema Pre-check (Java)

## Why This Exists

Schema evolution is useful, but the most important mismatch is between two different schema models:

- Schema Registry model: designed to validate and store schemas by format (Avro / JSON Schema / Protobuf)
- Iceberg model: designed around Iceberg table types and file-format-oriented constraints

A schema can be fully valid in Schema Registry and still fail when the Iceberg Kafka Connect Sink maps it into Iceberg/Parquet types.
Language-level expressiveness differences matter, but this tool prioritizes the Registry-to-Iceberg model gap.

This project exists to catch those failures early.
Instead of discovering issues at runtime in a connector task, you can paste a schema into a web UI and run a pre-check.

## What This Tool Does

This is a web application that validates whether a schema is likely to be convertible for Iceberg sink usage.

- Input formats: Avro, JSON Schema, Protobuf
- Output: pass/fail, warnings, errors, and a mapped Iceberg-style schema view
- Goal: fail fast during design/review, before deployment

## Design Principles

- Start with official libraries where possible.
- Focus checks on Schema Registry semantics vs Iceberg semantics.
- Keep checks practical for pre-validation, not a perfect emulator of every connector edge case.
- Show actionable diagnostics (path + message), not only a boolean result.

## Technical Stack

- Spring Boot (Web)
- Confluent Schema Registry libraries
  - io.confluent:kafka-avro-serializer
  - io.confluent:kafka-json-schema-serializer
  - io.confluent:kafka-protobuf-serializer
- Apache Iceberg
  - org.apache.iceberg:iceberg-core
  - org.apache.iceberg:iceberg-parquet

## How Conversion Is Checked

- Avro: parsed by Confluent Avro classes, then converted to Iceberg via AvroSchemaUtil.
- JSON Schema: parsed and validated, then mapped to Iceberg-compatible types with rule-based checks.
- Protobuf: parsed into descriptors, then mapped to Iceberg-compatible types with rule-based checks.

## Quick Start

1. Run the application:

   mvn spring-boot:run

2. Open the UI:

   http://localhost:8080

3. Paste a schema, select format, and submit.

## Docker

1. Build image:

   docker build -t your-registry/iceberg-sr-checker:latest .

2. Push image:

   docker push your-registry/iceberg-sr-checker:latest

## Testing

Run tests with:

mvn test

## Scope and Limitations

This tool is a pre-check utility.
It is intentionally designed to identify common Registry-to-Iceberg incompatibilities early, but it does not guarantee perfect behavior parity with every production connector configuration.