from pyspark.sql.functions import current_timestamp, to_date, lit, col

def add_bronze_metadata(df,source_type):
    df = df.withColumn("_ingestion_timestamp", current_timestamp()) \
        .withColumn("ingestion_date", to_date("_ingestion_timestamp"))

    if source_type == "streaming":
        return df.withColumn("_source_file", lit("confluent_kafka_stream"))
    else:
        return df.withColumn("_source_file", col("_metadata.file_path"))