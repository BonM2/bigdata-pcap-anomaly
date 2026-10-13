FROM apache/spark:3.5.0
USER root
# Match the Hadoop 3.3.4 jars bundled with Spark 3.5.0.
RUN curl -fsSL https://repo.maven.apache.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
      -o /opt/spark/jars/hadoop-aws-3.3.4.jar \
    && curl -fsSL https://repo.maven.apache.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
      -o /opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar \
    && python3 -m pip install --no-cache-dir boto3==1.34.0
ENV PYTHONPATH=/workspace:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip
ENV SPARK_PACKAGES=""
WORKDIR /workspace
USER spark
