from pyspark.sql.functions import current_timestamp, input_file_name, to_date

def add_bronze_metadata(df):
    return df.withColumn("_ingestion_timestamp", current_timestamp()) \
             .withColumn("ingestion_date", to_date("_ingestion_timestamp")) \
             .withColumn("_source_file", input_file_name())