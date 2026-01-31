FROM bitnami/spark:3.5.0

WORKDIR /workspace
COPY pipelines/pyspark /workspace/pipelines/pyspark

ENTRYPOINT ["spark-submit"]
CMD ["--help"]
