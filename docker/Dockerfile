# Use Python 3.12 with UV pre-installed
FROM ghcr.io/astral-sh/uv:python3.12-slim

# Set working directory
WORKDIR /app

# Copy dependency files first (for better caching)
COPY pyproject.toml ./

# Install dependencies using UV
RUN uv sync --no-dev

# Create scripts directory
RUN mkdir -p /scripts

# Copy the scripts directory from the host
COPY scripts/ /scripts/

# Set default environment variable for script name
ENV SCRIPT_NAME=""

# Create entrypoint script
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Set the entrypoint
ENTRYPOINT ["/entrypoint.sh"]