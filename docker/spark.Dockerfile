FROM apache/spark:3.5.8-scala2.12-java11-python3-r-ubuntu

WORKDIR /workspace
COPY pipelines/pyspark /workspace/pipelines/pyspark

ENTRYPOINT ["/opt/spark/bin/spark-submit"]
CMD ["--help"]
