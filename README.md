# ExaDigiT Simulation Server

REST API that allows running and querying the results from the ExaDigit simulation and Raps.
Currently hosted at https://obsidian.ccs.ornl.gov/exadigit/api/
You can see the docs for the API at https://obsidian.ccs.ornl.gov/exadigit/api/docs

## Deploying
To deploy the server, run
```bash
./deploy.sh bash
```

This will build both the server and simulation docker images, and push them to Slate.
