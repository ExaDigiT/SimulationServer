#!/bin/bash
# Deploy the pod. Pass the environment (prod or stage) you want to deploy to
set -e # Exit if any commmand fails

SCRIPT_DIR=$(realpath $(dirname "${BASH_SOURCE[0]}"))
cd "$SCRIPT_DIR"
ENV=$1
if [ "$ENV" != "prod" ] && [ "$ENV" != "stage" ]; then
    echo 'You need to pass either "prod" or "stage"'
fi
SERVER_IMAGE="registry.apps.marble.ccs.ornl.gov/stf218-app/exadigit-simulation-server"
JOB_IMAGE="registry.apps.marble.ccs.ornl.gov/stf218-app/exadigit-simulation-server-simulation-job"

docker build -t $SERVER_IMAGE:latest-$ENV -f Dockerfile.server .
docker build -t $JOB_IMAGE:latest-$ENV -f Dockerfile.simulation .
docker push $SERVER_IMAGE:latest-$ENV
docker push $JOB_IMAGE:latest-$ENV
if [[ "$ENV" = "prod" ]]; then # Alias latest-prod to latest
    docker tag $SERVER_IMAGE:latest-prod $SERVER_IMAGE:latest
    docker tag $JOB_IMAGE:latest-prod $JOB_IMAGE:latest
    docker push $SERVER_IMAGE:latest
    docker push $JOB_IMAGE:latest
fi

# Scale down so pod gets recreated and uses new image. Allow error if pod doesn't exist
oc --namespace stf218-app scale deploy -l env=$ENV,app=exadigit-simulation-server --replicas=0 || true 

# Process template and apply
oc process -f ./deployment.yaml -o yaml --param=ENV=$ENV | oc apply -f -
