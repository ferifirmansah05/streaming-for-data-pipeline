from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    from_json, col, to_timestamp, window, expr, sum, 
    when, lit, current_timestamp, row_number
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = (
    SparkSession.builder
    .appName("Transaction Streaming Processor")
    .config("spark.streaming.stopGracefullyOnShutdown", True)
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"
    )
    .master("local[*]")
    .getOrCreate()
)

# Set level log ke WARN agar output konsol bersih dari log info
spark.sparkContext.setLogLevel("WARN")

# 2. Definisi Schema JSON Event
transaction_schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("amount", IntegerType(), True),
    StructField("timestamp", StringType(), True),
    StructField("source", StringType(), True)
])

# 3. Membaca Data Stream dari Kafka Topic transactions
raw_stream_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "transactions")
    .option("startingOffsets", "earliest")
    .load()
)

# Deserialize JSON dan ubah string timestamp menjadi tipe data Timestamp (Type Validation)
parsed_stream_df = raw_stream_df.selectExpr("CAST(value AS STRING) AS json_val") \
    .select(from_json(col("json_val"), transaction_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'"))

# 4. Lakukan 5 Validasi Wajib
# Menambahkan kolom error_reason awal dan flag is_valid' sementara
validated_stream_df = parsed_stream_df \
    .withColumn(
        "error_reason",
        when(col("user_id").isNull() | col("amount").isNull() | col("timestamp").isNull(), "Missing mandatory fields")
        .when(col("event_time").isNull(), "Invalid timestamp format")
        .when(~col("amount").between(1, 10000000), "Amount out of range")
        .when(~col("source").isin("mobile", "web", "pos"), "Unknown source")
        .otherwise("None")
    )

# 5. Fungsi Pemrosesan per Micro-Batch (foreachBatch)
def process_micro_batch(df_batch, epoch_id):
    if df_batch.isEmpty():
        return

    # A. Deteksi Duplikat menggunakan Window function row_number() berbasis user_id + event_time
    window_spec = Window.partitionBy("user_id", "event_time").orderBy("event_time")
    
    # Jika baris ke-2 atau lebih ditemukan pada kombinasi key yang sama, tandai sebagai duplicate
    df_with_dup_check = df_batch.withColumn("rn", row_number().over(window_spec)) \
        .withColumn(
            "error_reason",
            when((col("rn") > 1) & (col("error_reason") == "None"), "Duplicate event")
            .otherwise(col("error_reason"))
        ) \
        .withColumn("is_valid", expr("error_reason == 'None'")) \
        .drop("rn")

    # B. Routing Output Data
    df_valid = df_with_dup_check.filter(col("is_valid") == True)
    df_invalid = df_with_dup_check.filter(col("is_valid") == False)

    # C. Tulis Data Valid ke Kafka Topic: transactions_valid
    if not df_valid.isEmpty():
        df_valid.selectExpr("CAST(user_id AS STRING) AS key", "to_json(struct(*)) AS value") \
            .write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("topic", "transactions_valid") \
            .mode("append") \
            .save()

    # D. Tulis Data Invalid (Gagal Validasi & Duplikat) ke Kafka Topic: transactions_dlq
    if not df_invalid.isEmpty():
        df_invalid.selectExpr("CAST(user_id AS STRING) AS key", "to_json(struct(*)) AS value") \
            .write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:9092") \
            .option("topic", "transactions_dlq") \
            .mode("append") \
            .save()

    # E. Terapkan Watermark & Tumbling Window (Monitoring) pada Data Valid
    windowed_summary = df_valid \
        .groupBy(window(col("event_time"), "1 minute")) \
        .agg(expr("count(1)").alias("total_transactions"))

    # F. Hitung Total Kumulatif (Running Total) & Tampilkan ke Console
    if not windowed_summary.isEmpty():
        # Window spec untuk menghitung running total kumulatif berdasarkan waktu akhir window
        running_total_window = Window.orderBy("window.end").rowsBetween(Window.unboundedPreceding, Window.currentRow)
        
        final_console_df = windowed_summary \
            .withColumn("timestamp", current_timestamp()) \
            .withColumn("running_total", sum("total_transactions").over(running_total_window)) \
            .select("timestamp", "window.start", "window.end", "total_transactions", "running_total") \
            .orderBy("window.start")
        
        print(f"\n=======================================================")
        print(f"📊 MONITORING OUTPUT - BATCH ID: {epoch_id}")
        print(f"=======================================================")
        final_console_df.show(truncate=False)


# 6. Menerapkan Watermark global pada Stream (Batas toleransi late data 3 menit)
stream_with_watermark = validated_stream_df.withWatermark("event_time", "3 minutes")

# 7. Menjalankan Sink Utama menggunakan foreachBatch
query = (
    stream_with_watermark.writeStream
    .outputMode("update")
    .foreachBatch(process_micro_batch)
    .option("checkpointLocation", "chk-point-dir") # Ubah direktori sesuai OS Anda
    .start()
)

print("🚀 PySpark Structured Streaming Job is running... Waiting for data from Kafka.")
query.awaitTermination()