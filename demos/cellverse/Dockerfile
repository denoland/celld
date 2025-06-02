# docker build -t 588642314931.dkr.ecr.us-east-2.amazonaws.com/deno-cells:ecs -f Dockerfile.ecs .
# aws ecr get-login-password --region us-east-2 | docker login --username AWS --password-stdin 588642314931.dkr.ecr.us-east-2.amazonaws.com
# docker push 588642314931.dkr.ecr.us-east-2.amazonaws.com/deno-cells:ecs

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
    && mkdir -p /etc/apt/keyrings \
    && curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg \
    && echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_20.x nodistro main" | tee /etc/apt/sources.list.d/nodesource.list \
    && apt-get update \
    && rm -rf /var/lib/apt/lists/*

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
# The CMD from your original image will be passed as arguments to this
# entrypoint script. If your original image had
# `CMD ["celld", "--some-default-arg"]`, then "$@" in the script will become
# "celld" "--some-default-arg". If your original image's CMD was just the celld
# binary, then you might need to specify it in the exec line:
# exec /usr/local/bin/celld "$@"
# If `celld` was the CMD in the original image, and that CMD is what you want to
# run, the `exec celld "$@"` in the script should work assuming `celld` is in
# the PATH. If original image had ENTRYPOINT ["celld"] and CMD
# ["--default-args"], the last line of script should be:
# exec celld --default-args "$@" (or just exec celld "$@" if CMD was part of
# default args to entrypoint)
# Often, the original CMD is simply passed as arguments to the new ENTRYPOINT.
# If the original image had CMD ["celld"], then `exec celld "$@"` in the script
# is fine.
