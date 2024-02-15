FROM python:3.9

# RUN apt-get update \
#   && apt-get install \
#   && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt /app
RUN python -m pip install --no-cache-dir --upgrade pip
RUN python -m pip install --no-cache-dir -r requirements.txt
COPY ["main.py", "/app/"]
COPY ["src", "/app/src"]

CMD ["python", "main.py"]
