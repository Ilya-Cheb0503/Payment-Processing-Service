#!/bin/sh
set -eu

API_URL="${API_URL:-http://localhost:8000}"
API_KEY="${API_KEY:-dev-api-key-change-me}"
WEBHOOK_URL="${WEBHOOK_URL:-http://webhook-mock:8080/webhook}"
MOCK_EVENTS_URL="${MOCK_EVENTS_URL:-http://localhost:8080/events}"

echo "Waiting for API at ${API_URL}/health"
i=0
while [ "$i" -lt 60 ]; do
  if curl -fsS "${API_URL}/health" >/dev/null 2>&1; then
    break
  fi
  i=$((i + 1))
  sleep 2
done
curl -fsS "${API_URL}/health" >/dev/null

KEY="smoke-$(date +%s)"
echo "Creating payment with Idempotency-Key=${KEY}"
CREATE=$(curl -fsS -X POST "${API_URL}/api/v1/payments" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: ${API_KEY}" \
  -H "Idempotency-Key: ${KEY}" \
  -d "{\"amount\":\"1500.50\",\"currency\":\"RUB\",\"description\":\"Smoke payment\",\"metadata\":{\"order_id\":\"smoke\"},\"webhook_url\":\"${WEBHOOK_URL}\"}")
echo "${CREATE}"

PAYMENT_ID=$(python -c "import json,sys; print(json.loads(sys.argv[1])['payment_id'])" "${CREATE}")
echo "Polling payment ${PAYMENT_ID}"
STATUS="pending"
i=0
while [ "$i" -lt 40 ]; do
  BODY=$(curl -fsS "${API_URL}/api/v1/payments/${PAYMENT_ID}" -H "X-API-Key: ${API_KEY}")
  STATUS=$(python -c "import json,sys; print(json.loads(sys.argv[1])['status'])" "${BODY}")
  echo "status=${STATUS}"
  if [ "${STATUS}" != "pending" ]; then
    echo "${BODY}"
    break
  fi
  i=$((i + 1))
  sleep 1
done

if [ "${STATUS}" = "pending" ]; then
  echo "Payment stayed pending"
  exit 1
fi

if curl -fsS "${MOCK_EVENTS_URL}" >/dev/null 2>&1; then
  echo "Webhook mock events:"
  curl -fsS "${MOCK_EVENTS_URL}"
fi

echo "Smoke test passed"
