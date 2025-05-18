#!/bin/sh
set -e # Exit immediately if a command exits with a non-zero status.

ADVERTISE_IP_DISCOVERED=""
# Default port, can be overridden by ADVERTISE_PORT env var if you set that too
DEFAULT_PORT="8000" 
FINAL_ADVERTISE_PORT=${ADVERTISE_PORT:-$DEFAULT_PORT}

# Check if ADVERTISE_ADDR is already explicitly set by the user
if [ -n "$ADVERTISE_ADDR" ]; then
    echo "Using user-provided ADVERTISE_ADDR: $ADVERTISE_ADDR"
else
    # Attempt to discover IP if running on AWS ECS/Fargate
    if [ -n "$ECS_CONTAINER_METADATA_URI_V4" ]; then
        echo "AWS Fargate environment detected. Attempting to discover private IP..."
        METADATA_URL="${ECS_CONTAINER_METADATA_URI_V4}/task"

        # Fetching metadata. Using -f to fail silently on server errors for
        # curl.  The jq query extracts
        # Containers[0].Networks[0].IPv4Addresses[0] It will return 'null' as a
        # string if the path doesn't exist, so we check for that.
        IP_FROM_METADATA=$(curl -s -f "$METADATA_URL" | jq -r '.Containers[0].Networks[0].IPv4Addresses[0] // empty')

        if [ -n "$IP_FROM_METADATA" ] && [ "$IP_FROM_METADATA" != "null" ] && [ "$IP_FROM_METADATA" != "empty" ]; then
            ADVERTISE_IP_DISCOVERED=$IP_FROM_METADATA
            echo "Successfully discovered Fargate private IP: $ADVERTISE_IP_DISCOVERED"
            export ADVERTISE_ADDR="${ADVERTISE_IP_DISCOVERED}:${FINAL_ADVERTISE_PORT}"
            echo "Exported ADVERTISE_ADDR=${ADVERTISE_ADDR}"
        else
            echo "WARN: Failed to extract IP from Fargate metadata or IP was null/empty. Response from jq: '$IP_FROM_METADATA'"
            # Fallback or error handling if IP discovery is critical
        fi
    else
        echo "Not an AWS Fargate environment (ECS_CONTAINER_METADATA_URI_V4 not set)."
    fi

    # If ADVERTISE_ADDR is still not set (e.g. not on Fargate, discovery failed, and user didn't provide it)
    # celld will use its internal default or error if it requires it.
    if [ -z "$ADVERTISE_ADDR" ]; then
         echo "WARN: ADVERTISE_ADDR is not set. celld will rely on its defaults or potentially fail if external address is critical."
    fi
fi

# Check where celld is expected to be from the original image
echo "Executing celld with arguments: $@"
# Assume 'celld' is in the PATH of the original image, or use its full path if known.
exec celld "$@"
