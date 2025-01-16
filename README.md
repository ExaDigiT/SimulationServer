# ExaDigiT Simulation Server

REST API that allows running and querying the results from the ExaDigit simulation and RAPS.

## Downloading FMU models
You can run the job and power simulation without downloading any FMU models. But to use the cooling
simulation you'll need to download FMU models into the `models` directory. You can download
`Simulator_olcf5_base.fmu` from https://code.ornl.gov/exadigit/fmu-models if you have access. (The
FMU models aren't currently publicly available.)


## Deploying
To deploy the server, run
```bash
./scripts/deploy.sh prod
```

This will build both the server and simulation docker images, and push them to Slate.

## Running locally
To run a local version of the server run
```bash
./scripts/launch_local.sh
```
The server will be hosted on http://localhost:8080

You'll need at least 16 GiB of RAM, preferably 32 GiB for druid to run smoothly.

If you want to run replay data locally, you'll need to download the datasets (see ./scripts/fetch.sh)
and then ingest them in Druid. After launching, you can access the Druid UI at http://localhost:8888
and submit druid ingests for the system you want.

## API Docs
You can view the API docs and the `openapi.json` with the API specification at
https://exadigit.github.io/SimulationServer
