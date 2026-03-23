#!/usr/bin/env bash
set -euo pipefail

N=${1:-1000}
JAR="$(cd "$(dirname "$0")" && pwd)/build/libs/faro-e2e.jar"
FLINK_REST="${FLINK_REST:-http://localhost:8081}"
MAIN_CLASS="dev.faro.e2e.SensorPipelineJob"

if [ ! -f "$JAR" ]; then
  echo "JAR not found. Run: ./gradlew :faro-e2e:shadowJar" >&2
  exit 1
fi

OUTPUT_FILE="$(cd "$(dirname "$0")" && pwd)/output/faro-output.txt"
# shellcheck disable=SC2188
> "$OUTPUT_FILE"

echo "Uploading JAR to $FLINK_REST..."
UPLOAD=$(curl -sf -X POST -H "Expect:" -F "jarfile=@${JAR}" "${FLINK_REST}/jars/upload")
JAR_ID=$(echo "$UPLOAD" | grep -o '"filename":"[^"]*"' | sed 's|.*upload/||;s|"||g')
echo "Uploaded: $JAR_ID"

echo "Submitting job with N=$N..."
curl -sf \
  -X POST \
  -H "Content-Type: application/json" \
  -d "{\"entryClass\": \"${MAIN_CLASS}\", \"programArgs\": \"${N}\"}" \
  "${FLINK_REST}/jars/${JAR_ID}/run"

echo ""
echo "Done. View at ${FLINK_REST}"
echo "Capture events: docker compose -f faro-e2e/docker-compose.yml logs taskmanager -f"
