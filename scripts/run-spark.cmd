@echo off
cd /d C:\Users\janan\radar-streaming-platform
docker exec -it radar-spark /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --repositories https://repo1.maven.org/maven2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 /opt/spark/app/spark_consumer.py
