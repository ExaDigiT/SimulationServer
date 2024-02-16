#!/bin/bash
docker build -t registry.apps.marble.ccs.ornl.gov/stf218-app/exadigit-simulation-server:latest . &&
docker push registry.apps.marble.ccs.ornl.gov/stf218-app/exadigit-simulation-server:latest &&
# Scale down so pod gets recreated and uses new image
oc --namespace stf218-app scale deploy -l app=exadigit-simulation-server --replicas=0

oc apply -f deployment.yaml
