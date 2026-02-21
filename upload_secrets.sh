#!/bin/bash

# Upload CloudVolume secrets to Google Cloud Secret Manager as a bundled tar archive
# Similar to: kubectl create secret generic secrets --from-file=...

PROJECT_ID="${PROJECT_ID:-exalted-beanbag-334502}"
SECRETS_DIR="${HOME}/.cloudvolume/secrets"
SECRET_NAME="cloudvolume-secrets"

echo "Bundling CloudVolume secrets from: $SECRETS_DIR"
echo "Project: $PROJECT_ID"
echo "Secret name: $SECRET_NAME"

# Check if secrets directory exists
if [ ! -d "$SECRETS_DIR" ]; then
    echo "Error: Secrets directory not found: $SECRETS_DIR"
    exit 1
fi

# Create tar archive and upload to Secret Manager
echo "Creating tar archive and uploading..."
tar czf - -C "$SECRETS_DIR" . | gcloud secrets create "$SECRET_NAME" \
    --data-file=- \
    --project="$PROJECT_ID" \
    --replication-policy="automatic" 2>/dev/null

# If secret already exists, add a new version
if [ $? -ne 0 ]; then
    echo "Secret already exists, adding new version..."
    tar czf - -C "$SECRETS_DIR" . | gcloud secrets versions add "$SECRET_NAME" \
        --data-file=- \
        --project="$PROJECT_ID"
fi

if [ $? -eq 0 ]; then
    echo ""
    echo "✓ Secrets uploaded successfully!"
    echo ""
    echo "To grant your service account access to this secret, run:"
    echo ""
    echo "  gcloud secrets add-iam-policy-binding $SECRET_NAME \\"
    echo "    --member='serviceAccount:YOUR-SERVICE-ACCOUNT@$PROJECT_ID.iam.gserviceaccount.com' \\"
    echo "    --role='roles/secretmanager.secretAccessor' \\"
    echo "    --project='$PROJECT_ID'"
    echo ""
    echo "Or grant project-level access:"
    echo ""
    echo "  gcloud projects add-iam-policy-binding $PROJECT_ID \\"
    echo "    --member='serviceAccount:YOUR-SERVICE-ACCOUNT@$PROJECT_ID.iam.gserviceaccount.com' \\"
    echo "    --role='roles/secretmanager.secretAccessor'"
else
    echo "Error: Failed to upload secrets"
    exit 1
fi
