# ExaDigiT Simulation Server

REST API that allows running and querying the results from the ExaDigit simulation and RAPS.

## Loading RAPS and Dashboard submodules
This uses [RAPS](https://github.com/ExaDigiT/RAPS) to run the simulation, which is loaded as a 
submodule. The [Simulation Dashboard](https://github.com/ExaDigiT/SimulationDashboard) is also in a
separate repo and loaded as a submodule. Make to load the submodules by running:
```
git submodule update --init --recursive
```

## Downloading FMU models
The Frontier FMU models aren't currently publicly available. To run Frontier simulations with cooling enabled, use this
command to download them (if you have access to the fmu-models repo).
```
cd ./raps
make fetch-fmu-models
```

You can run the job and power simulation without downloading any FMU models. But to use the cooling
simulation you'll need to download FMU models into the `models` directory. You can download
`Simulator_olcf5_base.fmu` from https://code.ornl.gov/exadigit/fmu-models if you have access. (The
FMU models aren't currently publicly available.)

## Running locally
To run a local version of the server run
```bash
docker compose up --wait
```
The API server will be hosted on http://localhost:8081. The dashboard will be hosted on http://localhost:8080.

You'll need at least 32 GiB of RAM for druid and RAPS to run smoothly.

If you want to run replay data locally, you'll need to download the datasets and then ingest them in
Druid. You can fetch the datasets with `./scripts/fetch_data.sh`, and use the `./scripts/submit_data_ingests.py`
script to ingest them into druid.

View the server logs with:
```bash
docker compose logs -f --no-log-prefix simulation-server
```

To shut down the server run:
```bash
docker compose down
```

Use this if you want to wipe all the database data as well:
```bash
docker compose down --volumes
```

## Deploying
To deploy the server, run
```bash
./scripts/deploy.sh prod
```

This will build both the server and simulation docker images, and push them to Slate.

## API Docs
You can view the API docs and the `openapi.json` with the API specification at
https://exadigit.github.io/SimulationServer
