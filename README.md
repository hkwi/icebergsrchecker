# Iceberg Schema Pre-check

This application checks whether a Schema Registry schema can pass through the
same broad path used by the Iceberg Kafka Connect sink before data is persisted
as Parquet.

The default check does not require a sample value. It converts the source schema
to a Kafka Connect schema, maps that schema to an Iceberg schema, synthesizes a
minimal Iceberg record, and writes one temporary Parquet file.

## What This Tool Checks

- Schema Registry schema parsing for Avro, JSON Schema, and Protobuf.
- Confluent schema-to-Kafka-Connect schema conversion.
- Kafka Connect schema-to-Iceberg schema conversion using Iceberg sink-style
  rules.
- Parquet writer construction and one-record write using Iceberg's Parquet
  writer.

The response includes the Connect schema view, the mapped Iceberg schema view,
and the Parquet dry-run result.

## Sample Values

Sample values are optional.

When `sampleValue` is omitted, the checker writes a synthetic record generated
from the mapped Iceberg schema. This is the recommended default for Avro and
Protobuf because their JSON value encodings are easy to get wrong.

When `sampleValue` is provided, `sampleFormat` selects the value parser:

- Avro: Avro JSON encoding parsed with Avro's `GenericDatumReader`.
- JSON Schema: ordinary JSON passed through Confluent `JsonSchemaData`.
- Protobuf: Protobuf JSON or Protobuf TextFormat parsed into `DynamicMessage`.

This validates both the schema path and the provided sample data path.

## Request Shape

```json
{
  "format": "json-schema",
  "schema": "{\"type\":\"object\",\"properties\":{\"id\":{\"type\":\"string\"}}}",
  "schemaForceOptional": false,
  "converterConfig": {
    "use.optional.for.nonrequired": true
  },
  "sampleFormat": "json",
  "sampleValue": "{\"id\":\"A-1\"}"
}
```

`converterConfig` also accepts keys with the `value.converter.` prefix; the
prefix is stripped before passing the config to Confluent data conversion
classes.

## Quick Start

```sh
mvn spring-boot:run
```

Open the UI:

```text
http://localhost:8080
```

## Docker

```sh
docker build -t your-registry/iceberg-sr-checker:latest .
docker push your-registry/iceberg-sr-checker:latest
```

### Building Behind a Proxy

```sh
docker build \
  --build-arg=http_proxy=http://proxy.example.com:8080 \
  --build-arg=https_proxy=http://proxy.example.com:8080 \
  --build-arg=HTTP_PROXY=http://proxy.example.com:8080 \
  --build-arg=HTTPS_PROXY=http://proxy.example.com:8080 \
  --build-arg='MAVEN_OPTS=-Dmaven.resolver.transport=wagon -Dhttp.proxyHost=proxy.example.com -Dhttp.proxyPort=8080 -Dhttps.proxyHost=proxy.example.com -Dhttps.proxyPort=8080' \
  -t your-registry/iceberg-sr-checker:latest .
```

## Testing

```sh
mvn test
```

## Scope And Limitations

This is a local dry-run utility, not a replacement for a running Kafka Connect
task. It does not deserialize Confluent wire-format Kafka payloads or contact a
real Schema Registry. It checks the schema/data conversion path and temporary
Parquet write path inside the process.

Existing table compatibility and schema evolution against a live Iceberg catalog
are not checked unless those inputs are added to the API in the future.
