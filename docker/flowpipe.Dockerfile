ARG FLOWPIPE_BASE=ghcr.io/hurdad/flow-pipe-runtime:latest
FROM ${FLOWPIPE_BASE}

ENV FLOWPIPE_BIN=/opt/flow-pipe/bin/flow-pipe

WORKDIR /workspace
COPY pipelines/flowpipe /workspace/pipelines/flowpipe

ENTRYPOINT ["/bin/bash", "-lc"]
CMD ["${FLOWPIPE_BIN} --help"]
