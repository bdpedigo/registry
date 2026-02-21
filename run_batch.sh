#!/bin/bash

# Example usage script for running table_to_deltalake.py on Google Cloud Batch

# This script demonstrates how to build the container and submit a job

set -e

# Build and push the container image
echo "Building Docker image for production (linux/amd64)..."
CONTAINER_IMAGE="bdpedigo/cave-registry:latest"

# docker buildx build --platform linux/amd64 -f docker/Dockerfile -t "$CONTAINER_IMAGE" .

echo "Pushing Docker image to Docker Hub..."
# docker push "$CONTAINER_IMAGE"

# Set job parameters
export PROJECT_ID="exalted-beanbag-334502"
export REGION="us-west1"
export CONTAINER_IMAGE="$CONTAINER_IMAGE"

# Configure the table processing job
export DATASTACK="v1dd"
export TABLE_NAME="synapses_v1dd"
export VERSION="1196"
export OUT_PATH="gs://cave_annotation_bucket/cistern/${DATASTACK}/v${VERSION}/${TABLE_NAME}"
export MAT_DB_CLOUD_PATH="gs://cave_annotation_bucket/public/"
export N_ROWS_PER_CHUNK="700000000"
export PARTITION_COLUMN="post_pt_root_id"
export N_PARTITIONS="256"
export ZORDER_COLUMNS="post_pt_root_id,id"
export BLOOM_FILTER_COLUMNS="id"
export FPP="0.001"

# Machine configuration - adjust based on your data size
export MACHINE_TYPE="e2-highmem-16"  # 16 vCPUs, 128 GB RAM
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
sh ./submit_batch_job.sh