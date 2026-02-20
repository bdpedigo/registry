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