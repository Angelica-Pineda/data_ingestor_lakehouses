from pyspark.sql.functions import col
from pyspark.sql.avro.functions import from_avro  # Requiere paquete spark-avro
from src.common.metadata import add_bronze_metadata


class KafkaIngestor:
    def __init__(self, spark, kafka_conf, sr_conf, dataset_config):
        self.spark = spark
        self.kafka_options = {f"kafka.{k}": v for k, v in kafka_conf.items()}
        self.sr_conf = sr_conf
        self.config = dataset_config

    def ingest(self, schema_str):
        # Lectura de Kafka
        df = (self.spark.readStream
              .format("kafka")
              .options(**self.kafka_options)
              .option("subscribe", self.config['kafka_topic'])
              .option("startingOffsets", "earliest")
              .load())


        # Nota: En Bronze es común guardar el 'value' crudo y procesar en Silver,
        # pero aquí lo aplanamos para cumplir con el esquema esperado.
        decoded_df = df.select(
            from_avro(col("value"), schema_str, self.sr_conf).alias("data"),
            col("timestamp").alias("_kafka_timestamp")
        ).select("data.*", "_kafka_timestamp")

        # agregacion de metadatos
        bronze_df = add_bronze_metadata(decoded_df,"streaming")

        return (bronze_df.writeStream
                .format("delta")
                .option("checkpointLocation", self.config['checkpoint_path'])
                .trigger(processingTime='10 seconds')
                .toTable(self.config['target_table']))


