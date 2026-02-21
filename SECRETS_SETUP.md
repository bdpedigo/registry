# CloudVolume Secrets Setup for Google Batch

This setup is similar to Kubernetes secrets but uses Google Cloud Secret Manager.

## Quick Start

### 1. Upload Secrets (One-time setup)

Bundle and upload your CloudVolume secrets to Secret Manager:

```bash
./upload_secrets.sh
```

This creates a tar archive of `~/.cloudvolume/secrets/` and stores it in Secret Manager as `cloudvolume-secrets`.

### 2. Grant Service Account Access

Find your service account (the script will show this):

```bash
# For default Compute Engine service account
gcloud secrets add-iam-policy-binding cloudvolume-secrets \
  --member='serviceAccount:PROJECT-NUMBER-compute@developer.gserviceaccount.com' \
  --role='roles/secretmanager.secretAccessor' \
  --project='YOUR-PROJECT-ID'
```

### 3. Run Your Batch Job

```bash
export OUT_PATH='gs://your-bucket/output'
./submit_batch_job.sh
```

## How It Works

1. **upload_secrets.sh**: Bundles all files from `~/.cloudvolume/secrets/` into a tar.gz and uploads to Secret Manager
2. **entrypoint.sh**: When running on GCE/Batch, automatically:
   - Detects it's running on Google Cloud
   - Fetches the `cloudvolume-secrets` from Secret Manager
   - Extracts to `/root/.cloudvolume/secrets/`
   - Uses the secrets for authentication

## Comparison to Kubernetes

```bash
# Kubernetes approach
kubectl create secret generic secrets \
    --from-file=$HOME/.cloudvolume/secrets/file1.json \
    --from-file=$HOME/.cloudvolume/secrets/file2.json

# Our approach
tar czf - -C $HOME/.cloudvolume/secrets . | \
    gcloud secrets create cloudvolume-secrets --data-file=-
```

## Updating Secrets

To update secrets, just run `./upload_secrets.sh` again. It will create a new version of the secret.

## Manual Secret Access

To manually extract secrets from Secret Manager:

```bash
gcloud secrets versions access latest --secret=cloudvolume-secrets | \
    tar xzf - -C /path/to/extract/
```
