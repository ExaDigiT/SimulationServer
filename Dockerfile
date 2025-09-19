FROM python:3.12

RUN apt-get update \
  && apt-get install git libsnappy-dev \
  && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir hatch

WORKDIR /app

COPY pyproject.toml /app/
COPY raps/ /app/raps/
RUN hatch dep show requirements > /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY druid_ingests /app/druid_ingests
COPY simulation_server /app/simulation_server
COPY README.md /app/
RUN pip install --no-cache-dir -e .

# CMD ["python", "-m", "simulation_server.simulation.main"]
# CMD ["python", "-m", "simulation_server.server.main"]
