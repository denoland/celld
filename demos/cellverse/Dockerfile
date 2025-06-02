# docker build -t 588642314931.dkr.ecr.us-east-2.amazonaws.com/deno-cells:cellverse .
# aws ecr get-login-password --region us-east-2 | docker login --username AWS --password-stdin 588642314931.dkr.ecr.us-east-2.amazonaws.com
# docker push 588642314931.dkr.ecr.us-east-2.amazonaws.com/deno-cells:cellverse

# Accept base image tag as build argument
ARG CELL_BASE_IMAGE_TAG=latest
# Use your existing celld image as the base
FROM ghcr.io/denoland/cells:${CELL_BASE_IMAGE_TAG}

# Switch to root user to install packages
USER root

# Install curl, jq, and Node.js for building the Vite app
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        jq \
        ca-certificates \
        gnupg \

# Copy the entire project
COPY . /app
WORKDIR /app

# Copy production environment variables
COPY .prod.vars /app/.env

RUN deno install && deno run build

# Create data directories and copy built assets and source code
RUN mkdir -p /data/default/static && \
    cp -r dist/* /data/default/static/ && \
    mkdir -p /data/default/src && \
    cp -r cells/* /data/default/src/ && \
    mkdir -p /data && \
    chown -R celld:celld /data

COPY entrypoint-ecs.sh /usr/local/bin/entrypoint.sh

RUN chmod +x /usr/local/bin/entrypoint.sh

USER celld

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]