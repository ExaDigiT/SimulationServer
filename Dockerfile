FROM python:3.12.11

RUN apt-get update \
  && apt-get install git libsnappy-dev \
  && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir uv
ENV UV_NO_CACHE=true

WORKDIR /app

# Install RAPS dependencies as first layer for caching
COPY raps/pyproject.toml /app/raps/
RUN uv pip install --system -r /app/raps/pyproject.toml

# Install server dependencies (including raps) for caching
COPY raps/ /app/raps/
COPY pyproject.toml /app/
COPY README.md /app/
RUN uv pip install --system -r /app/pyproject.toml

# Install simulation server
COPY druid_ingests/ /app/druid_ingests/
COPY simulation_server/ /app/simulation_server/
RUN uv pip install --system -e .
# Re-install RAPS as editable (TODO: RAPS currently doesn't work in non-editable mode)
RUN uv pip install --system -e ./raps

CMD ["python", "-m", "simulation_server.server.main"]
