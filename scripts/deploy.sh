#!/bin/bash
# Deploy the pod. Pass the environment (prod or stage) you want to deploy to
set -e # Exit if any commmand fails
BASE_DIR=$(realpath $(dirname "${BASH_SOURCE[0]}")/..)
cd "$BASE_DIR"

REGISTRY="registry.apps.marble.ccs.ornl.gov/stf218-app"

ENV=$1
if [ "$ENV" != "prod" ] && [ "$ENV" != "stage" ]; then
    echo 'You need to pass either "prod" or "stage"'
    exit
fi

SERVER_IMAGE_STREAM="$REGISTRY/exadigit-simulation-server"
docker build -t $SERVER_IMAGE_STREAM:latest -f Dockerfile .
docker push $SERVER_IMAGE_STREAM:latest

SERVER_IMAGE=$(docker inspect --format='{{index .RepoDigests 0}}' $SERVER_IMAGE_STREAM:latest)
echo "$SERVER_IMAGE"

# Scale down so pod gets recreated and uses new image. Allow error if pod doesn't exist
oc --namespace stf218-app scale deploy -l env=$ENV,app=exadigit-simulation-server --replicas=0 || true 

# Process template and apply
oc process -f ./deployment.yaml -o yaml \
    --param=ENV=$ENV --param=SERVER_IMAGE="$SERVER_IMAGE" --param=JOB_IMAGE="$SERVER_IMAGE" \
    | oc apply -f -
