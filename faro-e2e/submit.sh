#!/usr/bin/env bash
set -euo pipefail

JAR="$(cd "$(dirname "$0")" && pwd)/build/libs/faro-e2e.jar"
FLINK_REST="${FLINK_REST:-http://localhost:8081}"
ENTRY_CLASS="${ENTRY_CLASS:-dev.faro.e2e.FaroSensorJob}"

if [ ! -f "$JAR" ]; then
  echo "JAR not found. Run: ./gradlew :faro-e2e:shadowJar" >&2
  exit 1
fi

> "$(cd "$(dirname "$0")" && pwd)/output/faro-output.txt"

echo "Uploading JAR..."
JAR_ID=$(curl -sf -X POST -H "Expect:" \
  -F "jarfile=@${JAR}" \
  "${FLINK_REST}/jars/upload" \
  | grep -o '"filename":"[^"]*"' | sed 's|.*upload/||;s|"||g')
echo "Uploaded: $JAR_ID"

echo "Submitting $ENTRY_CLASS (scenario=${FARO_SCENARIO:-ROTATING}, capture=${FARO_CAPTURE_MODE:-ENTITY})..."
curl -sf -X POST -H "Content-Type: application/json" \
  -d "{\"entryClass\": \"${ENTRY_CLASS}\"}" \
  "${FLINK_REST}/jars/${JAR_ID}/run"

echo ""
echo "Flink UI:   ${FLINK_REST}"
echo "faro-api:   http://localhost:9000/api/v1/features/temperature/health"
echo "Violations: http://localhost:9000/api/v1/violations"
echo "Output:     tail -f faro-e2e/output/faro-output.txt"
