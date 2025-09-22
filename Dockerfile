FROM python:3.12

RUN apt-get update \
  && apt-get install git libsnappy-dev \
  && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir hatch

WORKDIR /app

# Install RAPS dependencies as first layer for caching
COPY raps/pyproject.toml /app/raps/
RUN cd /app/raps && hatch dep show requirements > /app/raps/requirements.txt
RUN pip install --no-cache-dir -r /app/raps/requirements.txt

# Install server dependencies (including raps) for caching
COPY raps/ /app/raps/
COPY pyproject.toml /app/
RUN hatch dep show requirements > /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Install simulation server
COPY README.md /app/
COPY druid_ingests/ /app/druid_ingests/
COPY simulation_server/ /app/simulation_server/
RUN pip install --no-cache-dir -e .
# Re-install RAPS as editable (TODO: RAPS currently doesn't work in non-editable mode)
RUN pip install --no-cache-dir -e ./raps

# CMD ["python", "-m", "simulation_server.simulation.main"]
# CMD ["python", "-m", "simulation_server.server.main"]
