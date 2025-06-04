# NOTE: Make sure to download `.prod.vars` from 1password and put it in this directory with a read permission for all users.
#
# docker build -t 588642314931.dkr.ecr.us-east-2.amazonaws.com/deno-cells:cellverse .
# aws ecr get-login-password --region us-east-2 | docker login --username AWS --password-stdin 588642314931.dkr.ecr.us-east-2.amazonaws.com
# docker push 588642314931.dkr.ecr.us-east-2.amazonaws.com/deno-cells:cellverse

# Accept base image tag as build argument
ARG CELL_BASE_IMAGE_TAG=latest
# Use your existing celld image as the base
FROM ghcr.io/denoland/cells:${CELL_BASE_IMAGE_TAG}
#FROM cells

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        jq \
        ca-certificates \
        gnupg

COPY . /app
#COPY dist/ /data/default/static
#COPY cells/ /data/default/src
#COPY .prod.vars /app/.env
WORKDIR /app
RUN deno install && deno run build
RUN chown -R celld:celld /data
COPY entrypoint-ecs.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh
USER celld
ENTRYPOINT ["/usr/local/bin/entrypoint.sh", "--static-fallback=spa", "--env-file=/app/.prod.vars", "cells/main.ts", "dist/" ]
