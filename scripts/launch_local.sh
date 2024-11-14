#!/bin/bash
# Launch local version
set -e # Exit if any commmand fails

SCRIPT_DIR=$(realpath $(dirname "${BASH_SOURCE[0]}"))
cd "$SCRIPT_DIR"

docker build -t exadigit-simulation-server:latest -f Dockerfile.server .
# docker build -t exadigit-simulation-server-simulation-job:latest -f Dockerfile.simulation .

# trap 'docker compose down' SIGINT SIGTERM EXIT

docker stop simulation-server >/dev/null 2>&1 || true
docker compose up -d
docker compose logs -f --no-log-prefix simulation-server
