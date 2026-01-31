FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir pyyaml psutil

WORKDIR /workspace
COPY . /workspace

ENTRYPOINT ["python", "-m", "runners.run_benchmark"]
CMD ["--help"]
