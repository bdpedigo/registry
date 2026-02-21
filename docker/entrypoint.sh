#!/bin/bash

# Check if SCRIPT_NAME environment variable is set
if [ -z "$SCRIPT_NAME" ]; then
    echo "Error: SCRIPT_NAME environment variable is not set."
    echo "Please specify a script to run using: docker run -e SCRIPT_NAME=your_script.py your_image"
    echo "Available scripts in /scripts:"
    ls -la /scripts/
    exit 1
fi

# Check if the script exists
SCRIPT_PATH="/scripts/$SCRIPT_NAME"
if [ ! -f "$SCRIPT_PATH" ]; then
    echo "Error: Script '$SCRIPT_NAME' not found in /scripts/"
    echo "Available scripts:"
    ls -la /scripts/
    exit 1
fi

# Setup CloudVolume secrets directory
mkdir -p /root/.cloudvolume/secrets

# Check if running on GCE/Batch and fetch secrets from Secret Manager
if curl -s -H "Metadata-Flavor: Google" http://metadata.google.internal/computeMetadata/v1/instance/ > /dev/null 2>&1; then
    echo "Running on GCE/Batch - checking for secrets in Secret Manager..."
    
    # Try to fetch bundled secrets from Secret Manager
    if gcloud secrets versions access latest --secret=cloudvolume-secrets 2>/dev/null | tar xzf - -C /root/.cloudvolume/secrets/; then
        echo "✓ CloudVolume secrets extracted from Secret Manager"
    else
        echo "Note: No bundled secrets found in Secret Manager (cloudvolume-secrets)"
        echo "Will attempt to use VM service account for authentication"
    fi
fi

# Activate Google Cloud service account if credentials file exists
if [ -f "/root/.cloudvolume/secrets/google-secret.json" ]; then
    echo "Activating Google Cloud service account..."
    gcloud auth activate-service-account --key-file=/root/.cloudvolume/secrets/google-secret.json
    if [ $? -eq 0 ]; then
        echo "Google Cloud authentication successful."
    else
        echo "Warning: Google Cloud authentication failed, continuing anyway..."
    fi
else
    echo "Warning: Google Cloud service account file not found at /root/.cloudvolume/secrets/google-secret.json"
fi

# Run the script using UV
echo "Running script: $SCRIPT_NAME"
exec uv run python "$SCRIPT_PATH" "$@"