#!/bin/bash
# Deploy the pod. Pass the environment (prod or stage) you want to deploy to
set -e # Exit if any commmand fails

REGISTRY="registry.apps.marble.ccs.ornl.gov/stf218-app"

SCRIPT_DIR=$(realpath $(dirname "${BASH_SOURCE[0]}"))
cd "$SCRIPT_DIR"
ENV=$1
if [ "$ENV" != "prod" ] && [ "$ENV" != "stage" ]; then
    echo 'You need to pass either "prod" or "stage"'
    exit
fi
SERVER_IMAGE_STREAM="$REGISTRY/exadigit-simulation-server"
JOB_IMAGE_STREAM="$REGISTRY/exadigit-simulation-server-simulation-job"

docker build -t $SERVER_IMAGE_STREAM:latest -f Dockerfile.server .
docker build -t $JOB_IMAGE_STREAM:latest -f Dockerfile.simulation .
docker push $SERVER_IMAGE_STREAM:latest
docker push $JOB_IMAGE_STREAM:latest

SERVER_IMAGE=$(docker inspect --format='{{index .RepoDigests 0}}' $SERVER_IMAGE_STREAM:latest)
JOB_IMAGE=$(docker inspect --format='{{index .RepoDigests 0}}' $JOB_IMAGE_STREAM:latest)

# Scale down so pod gets recreated and uses new image. Allow error if pod doesn't exist
oc --namespace stf218-app scale deploy -l env=$ENV,app=exadigit-simulation-server --replicas=0 || true 

# Process template and apply
oc process -f ./deployment.yaml -o yaml \
    --param=ENV=$ENV --param=SERVER_IMAGE="$SERVER_IMAGE" --param=JOB_IMAGE="$JOB_IMAGE" \
    | oc apply -f -
