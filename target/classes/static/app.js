const examples = {
  avro: JSON.stringify(
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
  "json-schema": JSON.stringify(
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
  protobuf: `syntax = "proto3";

message OrderEvent {
  string id = 1;
  double amount = 2;
  repeated string tags = 3;
}`
};

const form = document.getElementById("checker-form");
const formatEl = document.getElementById("format");
const schemaEl = document.getElementById("schema");
const resultEl = document.getElementById("details");
const summaryEl = document.getElementById("summary");
const pillEl = document.getElementById("status-pill");
const exampleBtn = document.getElementById("example-btn");

exampleBtn.addEventListener("click", () => {
  schemaEl.value = examples[formatEl.value] || "";
});

form.addEventListener("submit", async (event) => {
  event.preventDefault();

  pillEl.className = "pill";
  pillEl.textContent = "Checking...";
  summaryEl.textContent = "変換可否をチェック中...";
  resultEl.textContent = "";

  try {
    const response = await fetch("/api/check", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        format: formatEl.value,
        schema: schemaEl.value
      })
    });
    const data = await response.json();

    if (data.ok) {
      pillEl.className = "pill ok";
      pillEl.textContent = "PASS";
      summaryEl.textContent = `変換可能です (${data.elapsedMs}ms)。`;
    } else {
      pillEl.className = "pill ng";
      pillEl.textContent = "FAIL";
      summaryEl.textContent = `変換できない要素があります (${data.elapsedMs}ms)。`;
    }

    resultEl.textContent = JSON.stringify(data, null, 2);
  } catch (err) {
    pillEl.className = "pill ng";
    pillEl.textContent = "ERROR";
    summaryEl.textContent = "チェック処理でエラーが発生しました。";
    resultEl.textContent = String(err);
  }
});

schemaEl.value = examples.avro;
