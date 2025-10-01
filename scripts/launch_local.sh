#!/bin/bash
set -e # Exit if any commmand fails
cd $(realpath $(dirname "${BASH_SOURCE[0]}")/..)

# trap 'docker compose down' SIGINT SIGTERM EXIT

docker compose down
docker compose up -d
docker compose logs -f --no-log-prefix simulation-server
