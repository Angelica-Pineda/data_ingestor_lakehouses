from pathlib import Path
from databricks.connect import DatabricksSession
from src.batch.batch_ingestor import AutoLoaderIngestor
from src.common.config_loader import load_concluent_config, load_datasets
from src.streaming.streaming_ingestor import KafkaIngestor


def main():

    spark = DatabricksSession.builder.getOrCreate()

    BASE_DIR = Path(__file__).resolve().parent.parent

    kafka_root = BASE_DIR / "config" / "client.properties"
    schema_root = BASE_DIR / "config" / "client_schema.properties"
    datasets_root = BASE_DIR / "config" / "datasets.json"

    kafka_conf = load_concluent_config(str(kafka_root))
    schema_conf = load_concluent_config(str(schema_root))
    datasets = load_datasets(str(datasets_root))

    # Procesamiento batch
    for ds in datasets.get('batch_datasets', []):
        print(f"Iniciando ingesta BATCH: {ds['dataset_name']}")
        ingestor = AutoLoaderIngestor(spark, ds)
        ingestor.ingest()

    #procesamiento streaming
    for ds in datasets.get('streaming_dataset', []):
        print(f"Iniciando ingesta STREAMING: {ds['name']}")


        schema_path = f"config/schemas/{ds['name']}.avsc"
        with open(schema_path, "r") as f:
            schema_str = f.read()

        streaming_ingestor = KafkaIngestor(spark, kafka_conf, schema_conf, ds)
        streaming_ingestor.ingest(schema_str)


    if spark.streams.active:
        spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()