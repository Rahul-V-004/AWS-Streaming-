#!/bin/bash
# ============================================
# Build Lambda Layer for Kafka Producer
# ============================================
# This script packages kafka-python and aws-msk-iam-sasl-signer-python
# into a Lambda Layer ZIP file that you upload via the AWS Console.
#
# Usage:
#   chmod +x build_layer.sh
#   ./build_layer.sh
#
# Output:
#   kafka_layer.zip — upload this as a Lambda Layer
# ============================================

set -e

echo "Building Lambda Layer for Kafka producer..."

# Clean up any previous build
rm -rf layer_build kafka_layer.zip

# Create the layer directory structure (Lambda expects python/)
mkdir -p layer_build/python

# Install packages into the layer directory
pip install \
    kafka-python==2.0.2 \
    aws-msk-iam-sasl-signer-python==1.0.1 \
    -t layer_build/python \
    --platform manylinux2014_x86_64 \
    --only-binary=:all: \
    --python-version 3.11

# Create the ZIP
cd layer_build
zip -r ../kafka_layer.zip python/
cd ..

# Clean up
rm -rf layer_build

# Print result
echo ""
echo "============================================"
echo "Layer built: kafka_layer.zip"
echo "Size: $(du -h kafka_layer.zip | cut -f1)"
echo "============================================"
echo ""
echo "Next steps:"
echo "  1. Go to AWS Console → Lambda → Layers → Create layer"
echo "  2. Upload kafka_layer.zip"
echo "  3. Compatible runtimes: Python 3.11"
echo "  4. Attach the layer to your Lambda function"
