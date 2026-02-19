# Google Cloud Batch Setup for table_to_deltalake.py

This directory contains scripts and configuration for running the `table_to_deltalake.py` script on Google Cloud Batch.

## Files

- `submit_batch_job.sh` - Main script to submit jobs to Google Cloud Batch
- `docker/Dockerfile` - Main container image for running all scripts
- `docker/entrypoint.sh` - Entrypoint script that runs specified scripts
- `run_batch_example.sh` - Example usage script
- `scripts/table_to_deltalake.py` - The main processing script (now parameterized with environment variables)

## Setup

### 1. Prerequisites

- Google Cloud SDK installed and configured
- Docker installed
- Appropriate GCP permissions for Batch API, Container Registry, and Cloud Storage

### 2. Enable APIs

```bash
gcloud services enable batch.googleapis.com
gcloud services enable compute.googleapis.com
```

### 3. Build and Push Container

```bash
# Set your Docker Hub username
export DOCKER_USERNAME="your-docker-username"

# Build for production (linux/amd64) - required for Google Cloud Batch
docker buildx build --platform linux/amd64 -f docker/Dockerfile -t "$DOCKER_USERNAME/cave-registry:latest" .

# Push to Docker Hub (you'll need to login first: docker login)
docker push "$DOCKER_USERNAME/cave-registry:latest"
```

### Platform Considerations

- **Local Development**: Use `docker-compose.yml` (no platform constraint for Apple Silicon compatibility)
- **Production/Batch**: Use `docker-compose.prod.yml` or explicit `--platform linux/amd64` builds
- **Google Cloud Batch**: Requires `linux/amd64` platform

## Usage

### Local Development

For local development and testing on Apple Silicon:

```bash
# Use local docker-compose (no platform constraint)
SCRIPT_NAME=table_to_deltalake.py docker-compose run --rm registry
```

### Production Builds

For Google Cloud Batch deployment:

```bash
# Use production docker-compose OR explicit platform build
SCRIPT_NAME=table_to_deltalake.py docker-compose -f docker-compose.prod.yml run --rm registry

# OR build explicitly for linux/amd64
docker buildx build --platform linux/amd64 -f docker/Dockerfile -t your-image .
```

### Basic Usage

```bash
# Set required parameters
export PROJECT_ID="your-project-id"
export OUT_PATH="gs://your-bucket/output-path"
export CONTAINER_IMAGE="your-docker-username/cave-registry:latest"

# Submit the job
./submit_batch_job.sh
```

### Advanced Usage

You can customize all the parameterized values:

```bash
# Table configuration
export DATASTACK="v1dd"
export TABLE_NAME="connections_with_nuclei"
export VERSION="1196"
export MAT_DB_CLOUD_PATH="gs://cave_annotation_bucket/public/"

# Processing configuration  
export N_ROWS_PER_CHUNK="50000000"
export PARTITION_COLUMN="post_pt_root_id" 
export N_PARTITIONS="64"

# Output optimization
export ZORDER_COLUMNS="post_pt_root_id,id"
export BLOOM_FILTER_COLUMNS="id"
export FPP="0.001"

# Machine configuration
export MACHINE_TYPE="c2d-highmem-32"
export BOOT_DISK_SIZE_GB="200"

# Job configuration
export REGION="us-west1"
export JOB_NAME="my-custom-job-name"

./submit_batch_job.sh
```

### Environment Variables Reference

| Variable | Default | Description |
|----------|---------|-------------|
| `MAT_DB_CLOUD_PATH` | `gs://cave_annotation_bucket/public/` | Base path for materialized databases |
| `DATASTACK` | `v1dd` | Name of the datastack |
| `TABLE_NAME` | `connections_with_nuclei` | Table or view to process |
| `VERSION` | `1196` | Materialization version |
| `N_ROWS_PER_CHUNK` | `50000000` | Rows to process per chunk |
| `OUT_PATH` | **Required** | Output path for Delta Lake table |
| `PARTITION_COLUMN` | `post_pt_root_id` | Column for partitioning |
| `N_PARTITIONS` | `64` | Number of partitions |
| `ZORDER_COLUMNS` | `post_pt_root_id,id` | Comma-separated Z-order columns |
| `BLOOM_FILTER_COLUMNS` | `id` | Comma-separated bloom filter columns |
| `FPP` | `0.001` | False positive probability for bloom filters |

### Machine Types

Common machine types for different data sizes:

- **Small tables** (< 1GB): `c2d-standard-16` (16 vCPUs, 64 GB RAM)
- **Medium tables** (1-10GB): `c2d-highmem-16` (16 vCPUs, 128 GB RAM)  
- **Large tables** (10-100GB): `c2d-highmem-32` (32 vCPUs, 256 GB RAM)
- **Very large tables** (>100GB): `c2d-highmem-64` (64 vCPUs, 512 GB RAM)

## Monitoring

### Check Job Status
```bash
gcloud batch jobs list --location=us-west1
gcloud batch jobs describe JOB_NAME --location=us-west1
```

### View Logs
```bash
gcloud batch jobs describe JOB_NAME --location=us-west1 --format='value(status.taskGroups[0].instances[0].logUri)'
```

### Follow Live Logs
```bash
gcloud logging tail "resource.type=gce_instance AND labels.job_name=JOB_NAME" --project=PROJECT_ID
```

## Cost Optimization

- Use **preemptible instances** for non-urgent jobs (add `"provisioningModel": "SPOT"` to the allocation policy)
- Choose appropriate machine types - don't over-provision memory/CPU
- Monitor [Cloud Storage costs](https://cloud.google.com/storage/pricing) for large output datasets
- Consider using [committed use discounts](https://cloud.google.com/compute/docs/instances/committed-use-discounts-overview) for regular processing

## Troubleshooting

### Common Issues

1. **Permission denied accessing GCS**: Ensure your service account has Storage Admin permissions
2. **Out of memory**: Reduce `N_ROWS_PER_CHUNK` or increase machine memory
3. **Disk full**: Increase `BOOT_DISK_SIZE_GB` for large temporary files
4. **Container not found**: Ensure you've built and pushed the container image

### Debug Mode

Set these environment variables for additional debugging:

```bash
export VERBOSE="true"
export LOGGING_LEVEL="DEBUG"
```