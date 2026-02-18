#!/bin/bash

# Example usage script for running table_to_deltalake.py on Google Cloud Batch

# This script demonstrates how to build the container and submit a job

set -e

# Build and push the container image
echo "Building Docker image..."
CONTAINER_IMAGE="gcr.io/exalted-beanbag-334502/cave-registry-batch:latest"

docker build -f Dockerfile.batch -t "$CONTAINER_IMAGE" .

echo "Pushing Docker image to Google Container Registry..."
docker push "$CONTAINER_IMAGE"

# Set job parameters
export PROJECT_ID="exalted-beanbag-334502"
export REGION="us-west1"
export CONTAINER_IMAGE="$CONTAINER_IMAGE"

# Configure the table processing job
export DATASTACK="v1dd"
export TABLE_NAME="connections_with_nuclei"
export VERSION="1196"
export OUT_PATH="gs://your-output-bucket/deltalake-output/${DATASTACK}_${TABLE_NAME}_v${VERSION}"

# Machine configuration - adjust based on your data size
export MACHINE_TYPE="c2d-highmem-32"  # 32 vCPUs, 256 GB RAM
export BOOT_DISK_SIZE_GB="200"        # Increase if you need more local storage

# Optional: Customize processing parameters
# export N_ROWS_PER_CHUNK="100000000"  # Process more rows at once if you have more memory
# export N_PARTITIONS="128"            # More partitions for very large datasets
# export ZORDER_COLUMNS="post_pt_root_id,pre_pt_root_id,id"  # Add more columns for Z-ordering

echo "Submitting job with the following configuration:"
echo "  Datastack: $DATASTACK"
echo "  Table: $TABLE_NAME" 
echo "  Version: $VERSION"
echo "  Output: $OUT_PATH"
echo "  Machine: $MACHINE_TYPE"

# Submit the job
./submit_batch_job.sh