from datetime import datetime, timezone

import clickhouse_connect
from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, concat_ws, count, countDistinct, current_timestamp, from_json, lit, sha2, to_timestamp
from pyspark.sql.types import MapType, StringType, StructField, StructType


schema = StructType([
    StructField("client_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("entity_id", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField("payload", MapType(StringType(), StringType()), True),
    StructField("use_case_id", StringType(), True),
    StructField("source_topic", StringType(), True),
])


spark = (
    SparkSession.builder
    .appName("RADAR Spark Dedup Metrics")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribePattern", "raw_events|usecase_.*")
    .option("startingOffsets", "earliest")
    .load()
)


events_df = (
    raw_df
    .selectExpr("topic as kafka_topic", "CAST(value AS STRING) as json_value")
    .select("kafka_topic", from_json(col("json_value"), schema).alias("event"))
    .select("kafka_topic", "event.*")
    .withColumn("source_topic", coalesce(col("source_topic"), col("kafka_topic")))
    .withColumn("use_case_id", coalesce(col("use_case_id"), col("source_topic"), lit("raw_events")))
    .withColumn("event_ts", to_timestamp(col("event_time")))
    .withColumn("ingested_at", current_timestamp())
    .withColumn(
        "dedup_key",
        sha2(concat_ws(":", col("source_topic"), col("client_id"), col("event_type"), col("entity_id")), 256)
    )
)


deduped_df = (
    events_df
    .withWatermark("event_ts", "10 minutes")
    .dropDuplicates(["dedup_key"])
)


clean_query = (
    deduped_df.writeStream
    .format("parquet")
    .option("path", "/opt/spark/app/data/clean_events")
    .option("checkpointLocation", "/opt/spark/app/checkpoints/clean_events_v3")
    .outputMode("append")
    .start()
)


def write_metrics_to_clickhouse(batch_df, batch_id):
    metrics_df = (
        batch_df
        .groupBy("source_topic", "client_id")
        .agg(
            count("*").alias("raw_events"),
            countDistinct("dedup_key").alias("unique_events"),
        )
        .withColumn("duplicate_events", col("raw_events") - col("unique_events"))
        .withColumn("duplicate_ratio", col("duplicate_events") / col("raw_events"))
    )

    rows = metrics_df.collect()
    if not rows:
        return

    client = clickhouse_connect.get_client(
        host="clickhouse",
        port=8123,
        database="radar",
        username="radar_user",
        password="radar_pass",
    )

    batch_time = datetime.now(timezone.utc).replace(tzinfo=None)

    data = [
        [
            batch_time,
            row["source_topic"],
            row["client_id"],
            int(row["raw_events"]),
            int(row["unique_events"]),
            int(row["duplicate_events"]),
            float(row["duplicate_ratio"]),
        ]
        for row in rows
    ]

    client.insert(
        "client_event_metrics",
        data,
        column_names=[
            "batch_time",
            "use_case_topic",
            "client_id",
            "raw_events",
            "unique_events",
            "duplicate_events",
            "duplicate_ratio",
        ],
    )


metrics_query = (
    events_df.writeStream
    .foreachBatch(write_metrics_to_clickhouse)
    .option("checkpointLocation", "/opt/spark/app/checkpoints/client_event_metrics_v3")
    .start()
)


clean_query.awaitTermination()
metrics_query.awaitTermination()


