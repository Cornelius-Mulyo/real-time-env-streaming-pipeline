import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, lit, when
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "water_quality")

PG_HOST = os.getenv("POSTGRES_HOST", "postgres")
PG_DB = os.getenv("POSTGRES_DB", "waterdb")
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
PG_URL = f"jdbc:postgresql://{PG_HOST}:5432/{PG_DB}"

schema = StructType([
    StructField("event_time", StringType(), False),
    StructField("sensor_id", StringType(), False),
    StructField("location", StringType(), False),
    StructField("ph", DoubleType(), True),
    StructField("turbidity", DoubleType(), True),
    StructField("dissolved_oxygen", DoubleType(), True),
    StructField("temperature_c", DoubleType(), True),
    StructField("conductivity", DoubleType(), True),
])

def write_jdbc(df, table):
    (df.write
      .format("jdbc")
      .option("url", PG_URL)
      .option("dbtable", table)
      .option("user", PG_USER)
      .option("password", PG_PASS)
      .option("driver", "org.postgresql.Driver")
      .mode("append")
      .save())

def main():
    spark = (SparkSession.builder
             .appName("water-quality-streaming")
             .config("spark.sql.shuffle.partitions", "2")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "latest")
           .load())

    parsed = (raw.selectExpr("CAST(value AS STRING) AS json_str")
                .select(from_json(col("json_str"), schema).alias("d"))
                .select("d.*")
                .withColumn("event_time", to_timestamp(col("event_time"))))

    def write_readings(batch_df, batch_id):
        out = batch_df.select(
            col("event_time"),
            col("sensor_id"),
            col("location"),
            col("ph"),
            col("turbidity"),
            col("dissolved_oxygen"),
            col("temperature_c"),
            col("conductivity"),
        )
        write_jdbc(out, "sensor_readings")

    readings_q = (parsed.writeStream
                  .foreachBatch(write_readings)
                  .outputMode("update")
                  .option("checkpointLocation", "/tmp/checkpoints/readings")
                  .start())

    alerts = (parsed
              .withColumn("alert_type",
                          when(col("ph") < 6.5, lit("LOW_PH"))
                          .when(col("ph") > 8.5, lit("HIGH_PH"))
                          .when(col("turbidity") > 10.0, lit("HIGH_TURBIDITY"))
                          .when(col("dissolved_oxygen") < 5.0, lit("LOW_DO"))
                          .otherwise(lit(None)))
              .where(col("alert_type").isNotNull())
              .withColumn("threshold",
                          when(col("alert_type") == "LOW_PH", lit(6.5))
                          .when(col("alert_type") == "HIGH_PH", lit(8.5))
                          .when(col("alert_type") == "HIGH_TURBIDITY", lit(10.0))
                          .when(col("alert_type") == "LOW_DO", lit(5.0))
                          .otherwise(lit(None)))
              .withColumn("metric",
                          when(col("alert_type").isin("LOW_PH", "HIGH_PH"), col("ph"))
                          .when(col("alert_type") == "HIGH_TURBIDITY", col("turbidity"))
                          .when(col("alert_type") == "LOW_DO", col("dissolved_oxygen"))
                          .otherwise(lit(None)))
              .withColumn("message", lit("Threshold exceeded; investigate sensor/site."))
              .select(
                  col("event_time").alias("alert_time"),
                  "sensor_id", "location", "alert_type", "metric", "threshold", "message"
              ))

    def write_alerts(batch_df, batch_id):
        write_jdbc(batch_df, "sensor_alerts")

    alerts_q = (alerts.writeStream
                .foreachBatch(write_alerts)
                .outputMode("update")
                .option("checkpointLocation", "/tmp/checkpoints/alerts")
                .start())

    readings_q.awaitTermination()
    alerts_q.awaitTermination()

if __name__ == "__main__":
    main()