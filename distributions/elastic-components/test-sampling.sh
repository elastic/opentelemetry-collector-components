#!/bin/bash

# Test script for raw log sampling feature
# Usage: ./test-sampling.sh [error|info|both]

set -e

ENDPOINT="http://localhost:4318/v1/logs"

send_error_log() {
    echo "📤 Sending ERROR log (should be sampled at 10% rate)..."
    curl -X POST "$ENDPOINT" \
      -H "Content-Type: application/json" \
      -d '{
      "resourceLogs": [{
        "resource": {
          "attributes": [{
            "key": "service.name",
            "value": {"stringValue": "payment-service"}
          }]
        },
        "scopeLogs": [{
          "scope": {
            "name": "payment-processor"
          },
          "logRecords": [{
            "timeUnixNano": "'"$(date +%s)000000000"'",
            "severityText": "ERROR",
            "body": {
              "stringValue": "{\"message\": \"Payment processing failed\", \"level\": \"error\", \"user_id\": 12345, \"amount\": 99.99, \"error_code\": \"INSUFFICIENT_FUNDS\"}"
            },
            "attributes": [{
              "key": "http.method",
              "value": {"stringValue": "POST"}
            }, {
              "key": "http.url",
              "value": {"stringValue": "/api/v1/payments"}
            }]
          }]
        }]
      }]
    }'
    echo -e "\n✅ ERROR log sent\n"
}

send_info_log() {
    echo "📤 Sending INFO log (should NOT be sampled)..."
    curl -X POST "$ENDPOINT" \
      -H "Content-Type: application/json" \
      -d '{
      "resourceLogs": [{
        "resource": {
          "attributes": [{
            "key": "service.name",
            "value": {"stringValue": "payment-service"}
          }]
        },
        "scopeLogs": [{
          "scope": {
            "name": "payment-processor"
          },
          "logRecords": [{
            "timeUnixNano": "'"$(date +%s)000000000"'",
            "severityText": "INFO",
            "body": {
              "stringValue": "{\"message\": \"Payment processed successfully\", \"level\": \"info\", \"user_id\": 12345, \"amount\": 99.99, \"duration_ms\": 245}"
            },
            "attributes": [{
              "key": "http.method",
              "value": {"stringValue": "POST"}
            }, {
              "key": "http.url",
              "value": {"stringValue": "/api/v1/payments"}
            }]
          }]
        }]
      }]
    }'
    echo -e "\n✅ INFO log sent\n"
}

send_multiple() {
    echo "📤 Sending multiple logs to test sampling rate..."
    for i in {1..10}; do
        curl -s -X POST "$ENDPOINT" \
          -H "Content-Type: application/json" \
          -d '{
          "resourceLogs": [{
            "resource": {
              "attributes": [{
                "key": "service.name",
                "value": {"stringValue": "test-service"}
              }]
            },
            "scopeLogs": [{
              "scope": {
                "name": "test-scope"
              },
              "logRecords": [{
                "timeUnixNano": "'"$(date +%s)000000000"'",
                "severityText": "ERROR",
                "body": {
                  "stringValue": "{\"message\": \"Test error #'"$i"'\", \"level\": \"error\", \"test_id\": '"$i"'}"
                }
              }]
            }]
          }]
        }' > /dev/null
        echo -n "."
    done
    echo -e "\n✅ Sent 10 ERROR logs (expect ~1 to be sampled at 10% rate)\n"
}

send_loop() {
    echo "🔄 Starting continuous log stream (1 per second, Ctrl+C to stop)..."
    counter=1
    while true; do
        timestamp=$(date +%s)
        curl -s -X POST "$ENDPOINT" \
        --verbose \
          -H "Content-Type: application/json" \
          -d '{
          "resourceLogs": [{
            "resource": {
              "attributes": [{
                "key": "service.name",
                "value": {"stringValue": "continuous-test"}
              }]
            },
            "scopeLogs": [{
              "scope": {
                "name": "test-loop"
              },
              "logRecords": [{
                "timeUnixNano": "'"${timestamp}000000000"'",
                "body": {
                  "stringValue": "'"Hello world, message ${counter}"'"
                }
              }]
            }]
          }]
        }'
        echo "$(date '+%H:%M:%S') - Sent log #$counter"
        counter=$((counter + 1))
        sleep 1
    done
}

# Main
case "${1:-both}" in
    error)
        send_error_log
        ;;
    info)
        send_info_log
        ;;
    multiple)
        send_multiple
        ;;
    loop)
        send_loop
        ;;
    both)
        send_error_log
        sleep 1
        send_info_log
        ;;
    *)
        echo "Usage: $0 [error|info|both|multiple|loop]"
        exit 1
        ;;
esac

echo "💡 Check the collector output to see:"
echo "   - Both logs in production pipeline (processed with parsed JSON)"
echo "   - Only ERROR log in sampling pipeline (raw format, ~10% of the time)"
