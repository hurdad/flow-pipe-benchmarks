ARG FLOWPIPE_BASE=ghcr.io/hurdad/flow-pipe-runtime:latest
ARG SPARK_BASE=apache/spark:3.5.8-scala2.12-java11-python3-r-ubuntu

FROM ${FLOWPIPE_BASE} as flowpipe

FROM ${SPARK_BASE} as spark

FROM python:3.11-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-11-jre-headless \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir pyyaml psutil

COPY --from=flowpipe /opt/flow-pipe /opt/flow-pipe
COPY --from=spark /opt/spark /opt/spark

ENV PATH="/opt/flow-pipe/bin:/opt/spark/bin:${PATH}" \
    SPARK_HOME="/opt/spark" \
    FLOWPIPE_BIN="/opt/flow-pipe/bin/flow-pipe"

WORKDIR /workspace
COPY . /workspace

ENTRYPOINT ["python", "-m", "runners.run_benchmark"]
CMD ["--help"]
