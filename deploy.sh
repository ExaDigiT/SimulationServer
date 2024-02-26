#!/bin/bash
SCRIPT_DIR=$(realpath $(dirname "${BASH_SOURCE[0]}"))
cd "$SCRIPT_DIR"

docker build -t registry.apps.marble.ccs.ornl.gov/stf218-app/exadigit-simulation-server:latest -f Dockerfile.server . &&
docker build -t registry.apps.marble.ccs.ornl.gov/stf218-app/exadigit-simulation-server-simulation-job:latest -f Dockerfile.simulation . &&
docker push registry.apps.marble.ccs.ornl.gov/stf218-app/exadigit-simulation-server:latest &&
docker push registry.apps.marble.ccs.ornl.gov/stf218-app/exadigit-simulation-server-simulation-job:latest &&
# # Scale down so pod gets recreated and uses new image
oc --namespace stf218-app scale deploy -l app=exadigit-simulation-server --replicas=0

oc apply -f deployment.yaml
