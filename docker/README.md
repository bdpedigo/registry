# Docker Setup for Registry

This Docker setup allows you to run Python scripts using UV for dependency management. Scripts can be specified via environment variables without rebuilding the image.

## Building the Image

Build the Docker image from the project root:

```bash
# Basic build
docker build -f docker/Dockerfile -t registry .

# Build for specific platform (recommended for deployment)
docker buildx build --platform linux/amd64 -t registry . -f ./docker/Dockerfile
```

## Running Scripts

### Basic Usage

Run a script by setting the `SCRIPT_NAME` environment variable:

```bash
# Run the example script
docker run --rm -e SCRIPT_NAME=example.py registry

# Run a script with arguments
docker run --rm -e SCRIPT_NAME=your_script.py registry arg1 arg2 arg3

# Run interactively to debug
docker run --rm -it -e SCRIPT_NAME=example.py registry
```

### With Volume Mounts

For scripts that need access to credentials or local data:

```bash
# Mount CloudVolume secrets (if using CAVEclient)
docker run --rm --platform linux/amd64 \
  -v ~/.cloudvolume/secrets:/root/.cloudvolume/secrets \
  -e SCRIPT_NAME=your_script.py registry

# Mount additional data directories
docker run --rm \
  -v /path/to/your/data:/data \
  -v ~/.cloudvolume/secrets:/root/.cloudvolume/secrets \
  -e SCRIPT_NAME=your_script.py registry
```

### Using Docker Compose (Recommended)

For easier volume management, use the provided docker-compose.yml:

```bash
# Run with docker-compose
SCRIPT_NAME=example.py docker-compose run --rm registry

# With arguments
SCRIPT_NAME=your_script.py docker-compose run --rm registry arg1 arg2
```

## Adding New Scripts

1. Add your Python scripts to the `scripts/` directory
2. Rebuild the image to include the new scripts
3. Run using the script filename as `SCRIPT_NAME`

## Mounting Scripts at Runtime (Advanced)

To avoid rebuilding when adding scripts, you can mount the scripts directory:

```bash
docker run --rm -v $(pwd)/scripts:/scripts -e SCRIPT_NAME=example.py registry
```

## Docker Registry Operations

### Tagging and Pushing

```bash
# Tag for registry
docker tag registry your-username/registry:v1

# Push to Docker Hub
docker push your-username/registry:v1

# Tag with specific version
docker tag registry your-username/registry:latest
docker push your-username/registry:latest
```

## Environment Variables

- `SCRIPT_NAME`: Required. The name of the Python script to run (must be in `/scripts/`)
- Additional environment variables can be passed through docker run `-e` flags or docker-compose

## Dependencies

Dependencies are managed via `pyproject.toml` and installed using UV during the Docker build process.

## Docker Compose Configuration

The `docker-compose.yml` file provides convenient volume mounting and environment variable management:

```bash
# Basic usage
SCRIPT_NAME=example.py docker-compose run --rm registry

# With custom environment variables
SCRIPT_NAME=your_script.py ENV_VAR1=value1 docker-compose run --rm registry

# Override volumes (uncomment lines in docker-compose.yml as needed)
# - CloudVolume secrets: automatically mounted to /root/.cloudvolume/secrets
# - Data directory: uncomment to mount ./data to /data in container
# - Scripts directory: uncomment for development to mount ./scripts to /scripts
```

## Troubleshooting

- If `SCRIPT_NAME` is not set, the container will list available scripts and exit
- If the specified script doesn't exist, the container will show available scripts and exit
- All output from the entrypoint script helps debug issues
- Use `docker-compose logs` to view container logs
- Check volume mounts with `docker inspect <container_name>`

## Development Tips

1. **Script Development**: Uncomment the scripts volume mount in docker-compose.yml to avoid rebuilds during development
2. **Data Access**: Ensure your CloudVolume secrets are properly mounted if using CAVEclient
3. **Platform Compatibility**: The setup uses linux/amd64 platform for consistent deployment
4. **Resource Monitoring**: Use `docker stats` to monitor container resource usage