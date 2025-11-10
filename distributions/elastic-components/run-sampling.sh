#!/bin/bash

# Start the OpenTelemetry Collector with raw sampling configuration
# Usage: ./run-sampling.sh

set -e

BINARY="./_build/elastic-collector-with-pipeline-extension"
CONFIG="config.sampling.yaml"

# Check if binary exists
if [ ! -f "$BINARY" ]; then
    echo "❌ Collector binary not found at $BINARY"
    echo "💡 Build it first with: ../../.tools/builder --config manifest.yaml"
    exit 1
fi

# Check if config exists
if [ ! -f "$CONFIG" ]; then
    echo "❌ Config file not found at $CONFIG"
    exit 1
fi

# Validate config
echo "✅ Validating configuration..."
if ! "$BINARY" validate --config "$CONFIG"; then
    echo "❌ Configuration validation failed"
    exit 1
fi

echo ""
echo "🚀 Starting OpenTelemetry Collector with Raw Sampling"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "📊 Endpoints:"
echo "   OTLP gRPC:  localhost:4317"
echo "   OTLP HTTP:  localhost:4318"
echo "   Health:     localhost:13133"
echo "   Profiling:  localhost:1777"
echo ""
echo "📝 Pipelines:"
echo "   logs/intake   → Captures & processes all logs"
echo "   logs/prod     → Exports processed logs (no raw.id)"
echo "   logs/sampling → Exports raw ERROR logs (~10% sampled)"
echo ""
echo "🧪 Test with:"
echo "   ./test-sampling.sh both"
echo ""
echo "📖 Full docs: ../../docs/raw-sampling.md"
echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""

# Run the collector
exec "$BINARY" --config "$CONFIG"
