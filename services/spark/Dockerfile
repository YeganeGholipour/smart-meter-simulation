FROM apache/spark:4.0.1-scala2.13-java17-python3-r-ubuntu

USER root
WORKDIR /app

ENV HOME=/root
ENV PYTHONPATH=/app

COPY services/spark/requirements.txt ./requirements.txt

RUN pip install -r requirements.txt

COPY services/spark/ ./services/spark/

RUN mkdir -p $SPARK_HOME/jars
COPY services/spark/jars/*.jar $SPARK_HOME/jars/

CMD ["spark-submit","--master", "spark://spark-master:7077", "--deploy-mode", "client", "/app/services/spark/main.py"]