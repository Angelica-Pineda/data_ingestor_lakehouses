from src.common.metadata import add_bronze_metadata


class AutoLoaderIngestor:
    def __init__(self, spark, config):
        self.spark = spark
        self.config = config

    def ingest(self):
        print(f"Iniciando Auto Loader para: {self.config['dataset_name']}")

        query = (self.spark.readStream
                 .format("cloudFiles")
                 .option("cloudFiles.format", self.config['format'])
                 .option("cloudFiles.schemaLocation", self.config['checkpoint_path'] + "schema")
                 .options(**self.config.get('options', {}))
                 .load(self.config['source_path']))

        # Aplicacion de metadatos
        bronze_df = add_bronze_metadata(query)

        # Escritura en Delta (farmia_Bronze)
        return (bronze_df.writeStream
                .format("delta")
                .outputMode("append")
                .option("checkpointLocation", self.config['checkpoint_path'])
                .trigger(availableNow=True)
                .toTable(self.config['target_table']))