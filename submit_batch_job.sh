#!/bin/bash

# Google Cloud Batch job submission script for table_to_deltalake.py

# Set default values
PROJECT_ID="${PROJECT_ID:-exalted-beanbag-334502}"
REGION="${REGION:-us-west1}"
JOB_NAME="${JOB_NAME:-table-to-deltalake-$(date +%Y%m%d-%H%M%S)}"

# Machine configuration - adjust these as needed
MACHINE_TYPE="${MACHINE_TYPE:-c2d-highmem-32}"  # 32 vCPUs, 256 GB RAM
BOOT_DISK_SIZE_GB="${BOOT_DISK_SIZE_GB:-100}"   # Boot disk size
ADDITIONAL_DISK_SIZE_GB="${ADDITIONAL_DISK_SIZE_GB:-500}"  # Additional disk for processing large files

# Container image - you'll need to build and push this
CONTAINER_IMAGE="${CONTAINER_IMAGE:-bdpedigo/cave-registry:latest}"

# Service account for GCS access
SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT_EMAIL:-""}"

# Environment variables for the job
MAT_DB_CLOUD_PATH="${MAT_DB_CLOUD_PATH:-gs://cave_annotation_bucket/public/}"
DATASTACK="${DATASTACK:-v1dd}"
TABLE_NAME="${TABLE_NAME:-connections_with_nuclei}"
VERSION="${VERSION:-1196}"
N_ROWS_PER_CHUNK="${N_ROWS_PER_CHUNK:-50000000}"
OUT_PATH="${OUT_PATH}"  # Required - no default
PARTITION_COLUMN="${PARTITION_COLUMN:-post_pt_root_id}"
N_PARTITIONS="${N_PARTITIONS:-64}"
ZORDER_COLUMNS="${ZORDER_COLUMNS:-post_pt_root_id,id}"
BLOOM_FILTER_COLUMNS="${BLOOM_FILTER_COLUMNS:-id}"
FPP="${FPP:-0.001}"

# Validate required parameters
if [[ -z "$OUT_PATH" ]]; then
    echo "Error: OUT_PATH environment variable is required"
    echo "Example: export OUT_PATH='gs://my-bucket/deltalake-output'"
    exit 1
fi

echo "Submitting Google Cloud Batch job..."
echo "Project ID: $PROJECT_ID"
echo "Region: $REGION"
echo "Job Name: $JOB_NAME"
echo "Machine Type: $MACHINE_TYPE"
echo "Container Image: $CONTAINER_IMAGE"
echo "Output Path: $OUT_PATH"

# Set the project
gcloud config set project "$PROJECT_ID"

# Generate the batch job configuration
cat > "/tmp/${JOB_NAME}_config.json" << EOF
{
  "taskGroups": [
    {
      "taskSpec": {
        "runnables": [
          {
            "container": {
              "imageUri": "$CONTAINER_IMAGE",
              "commands": [
                "/bin/bash",
                "-c",
                "cd /workspace && python scripts/table_to_deltalake.py"
              ]
            }
          }
        ],
        "computeResource": {
          "cpuMilli": 31000,
          "memoryMib": 245760,
          "bootDiskMib": $(($BOOT_DISK_SIZE_GB * 1024))
        },
        "maxRetryCount": 1,
        "maxRunDuration": "21600s",
        "environment": {
          "variables": {
            "MAT_DB_CLOUD_PATH": "$MAT_DB_CLOUD_PATH",
            "DATASTACK": "$DATASTACK",
            "TABLE_NAME": "$TABLE_NAME",
            "VERSION": "$VERSION",
            "N_ROWS_PER_CHUNK": "$N_ROWS_PER_CHUNK",
            "OUT_PATH": "$OUT_PATH",
            "PARTITION_COLUMN": "$PARTITION_COLUMN",
            "N_PARTITIONS": "$N_PARTITIONS",
            "ZORDER_COLUMNS": "$ZORDER_COLUMNS",
            "BLOOM_FILTER_COLUMNS": "$BLOOM_FILTER_COLUMNS",
            "FPP": "$FPP"
          }
        }
      },
      "taskCount": 1,
      "parallelism": 1
    }
  ],
  "allocationPolicy": {
    "instances": [
      {
        "policy": {
          "machineType": "$MACHINE_TYPE",
          "provisioningModel": "STANDARD"
        }
      }
    ]$(if [[ -n "$SERVICE_ACCOUNT_EMAIL" ]]; then echo ",
    \"serviceAccount\": {
      \"email\": \"$SERVICE_ACCOUNT_EMAIL\"
    }"; fi)
  },
  "logsPolicy": {
    "destination": "CLOUD_LOGGING"
  }
}
EOF

# Create and submit the batch job
gcloud batch jobs submit "$JOB_NAME" \
    --location="$REGION" \
    --config="/tmp/${JOB_NAME}_config.json" \
    --project="$PROJECT_ID"

echo "Job submitted successfully!"
echo "Job configuration saved to: /tmp/${JOB_NAME}_config.json"
echo ""
echo "You can monitor the job with:"
echo "gcloud batch jobs describe $JOB_NAME --location=$REGION"
echo "gcloud batch jobs list --location=$REGION"
echo ""
echo "To get logs:"
echo "gcloud batch jobs describe $JOB_NAME --location=$REGION --format='value(status.taskGroups[0].instances[0].logUri)'"

# Optionally follow logs
read -p "Would you like to follow the job logs? (y/n): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Following logs for job $JOB_NAME..."
    # Wait a bit for the job to start
    sleep 10
    gcloud logging tail "resource.type=gce_instance AND labels.job_name=$JOB_NAME" --project="$PROJECT_ID"
fi