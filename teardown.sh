#!/usr/bin/env bash
set -euo pipefail

if ! docker ps > /dev/null 2>&1; then
    if [ "${IN_SG_DOCKER:-0}" = "0" ]; then
        export IN_SG_DOCKER=1
        exec sg docker -c "$0 $*"
    fi
fi

CLUSTER_NAME="my-cluster"

echo "Deleting Kind cluster '${CLUSTER_NAME}'..."
kind delete cluster --name "${CLUSTER_NAME}"
echo "Cluster deleted. All resources and memory freed."
