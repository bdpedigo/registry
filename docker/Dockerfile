# Use an official google cloud cli image as the base image
FROM gcr.io/google.com/cloudsdktool/google-cloud-cli:slim AS gcloud-stage

# Use Python 3.12 with UV pre-installed
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

# Set working directory
WORKDIR /app

# Copy Google Cloud CLI from the first stage
COPY --from=gcloud-stage /usr/lib/google-cloud-sdk /usr/lib/google-cloud-sdk
COPY --from=gcloud-stage /usr/bin/gcloud /usr/bin/gcloud
COPY --from=gcloud-stage /usr/bin/gsutil /usr/bin/gsutil
ENV PATH="/usr/lib/google-cloud-sdk/bin:${PATH}"

# # Install Google Cloud SDK (includes gsutil) early for better layer caching
# RUN apt-get update && \
#     apt-get install -y curl gnupg && \
#     echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
#     curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - && \
#     apt-get update && \
#     apt-get install -y google-cloud-cli && \
#     apt-get clean && \
#     rm -rf /var/lib/apt/lists/*

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