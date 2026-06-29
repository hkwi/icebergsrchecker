const examples = {
  avro: {
    schema: JSON.stringify(
      {
        type: "record",
        name: "OrderEvent",
        fields: [
          { name: "id", type: "string" },
          { name: "price", type: "double" },
          { name: "memo", type: ["null", "string"], default: null }
        ]
      },
      null,
      2
    ),
    sampleFormat: "avro-json",
    sample: JSON.stringify({ id: "A-1", price: 12.5, memo: null }, null, 2),
    config: {}
  },
  "json-schema": {
    schema: JSON.stringify(
      {
        type: "object",
        required: ["id"],
        properties: {
          id: { type: "string" },
          amount: { type: "number" },
          tags: {
            type: "array",
            items: { type: "string" }
          }
        }
      },
      null,
      2
    ),
    sampleFormat: "json",
    sample: JSON.stringify({ id: "A-1", amount: 12.5, tags: ["new"] }, null, 2),
    config: { "use.optional.for.nonrequired": true }
  },
  protobuf: {
    schema: `syntax = "proto3";

message OrderEvent {
  string id = 1;
  double amount = 2;
  repeated string tags = 3;
}`,
    sampleFormat: "protobuf-json",
    sample: JSON.stringify({ id: "A-1", amount: 12.5, tags: ["new"] }, null, 2),
    config: {}
  }
};

const form = document.getElementById("checker-form");
const formatEl = document.getElementById("format");
const schemaEl = document.getElementById("schema");
const sampleFormatEl = document.getElementById("sample-format");
const sampleValueEl = document.getElementById("sample-value");
const schemaForceOptionalEl = document.getElementById("schema-force-optional");
const converterConfigEl = document.getElementById("converter-config");
const resultEl = document.getElementById("details");
const summaryEl = document.getElementById("summary");
const pillEl = document.getElementById("status-pill");
const exampleBtn = document.getElementById("example-btn");

exampleBtn.addEventListener("click", () => {
  const example = examples[formatEl.value];
  schemaEl.value = example?.schema || "";
  sampleFormatEl.value = example?.sampleFormat || "auto";
  sampleValueEl.value = example?.sample || "";
  converterConfigEl.value = JSON.stringify(example?.config || {}, null, 2);
});

form.addEventListener("submit", async (event) => {
  event.preventDefault();

  pillEl.className = "pill";
  pillEl.textContent = "Checking...";
  summaryEl.textContent = "Parquet dry-run を実行中...";
  resultEl.textContent = "";

  let converterConfig = {};
  try {
    converterConfig = JSON.parse(converterConfigEl.value || "{}");
  } catch (err) {
    pillEl.className = "pill ng";
    pillEl.textContent = "ERROR";
    summaryEl.textContent = "Converter config JSON が不正です。";
    resultEl.textContent = String(err);
    return;
  }

  const body = {
    format: formatEl.value,
    schema: schemaEl.value,
    sampleFormat: sampleFormatEl.value,
    schemaForceOptional: schemaForceOptionalEl.checked,
    converterConfig
  };

  if (sampleValueEl.value.trim()) {
    body.sampleValue = sampleValueEl.value;
  }

  try {
    const response = await fetch("/api/check", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body)
    });
    const data = await response.json();

    if (data.ok) {
      pillEl.className = "pill ok";
      pillEl.textContent = "PASS";
      summaryEl.textContent = `Parquet dry-run に成功しました (${data.elapsedMs}ms)。`;
    } else {
      pillEl.className = "pill ng";
      pillEl.textContent = "FAIL";
      summaryEl.textContent = `Parquet dry-run で失敗しました (${data.elapsedMs}ms)。`;
    }

    resultEl.textContent = JSON.stringify(data, null, 2);
  } catch (err) {
    pillEl.className = "pill ng";
    pillEl.textContent = "ERROR";
    summaryEl.textContent = "チェック処理でエラーが発生しました。";
    resultEl.textContent = String(err);
  }
});

schemaEl.value = examples.avro.schema;
sampleFormatEl.value = "auto";
sampleValueEl.value = "";
converterConfigEl.value = "{}";
