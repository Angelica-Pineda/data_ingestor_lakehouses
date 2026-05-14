import json
import os


def load_concluent_config(path):
  """
  Lee el archivo .properties de Kafka y lo convierte en un diccionario.
  Maneja comentarios (#) y líneas vacías.
  """
  if not os.path.exists(path):
    raise FileNotFoundError(f"No se encontró el archivo de configuración de Kafka en: {path}")

  config = {}
  with open(path, "r") as fh:
    for line in fh:
      line = line.strip()
      # Ignoramos comentarios y líneas vacías
      if len(line) != 0 and not line.startswith("#"):
        parameter, value = line.split('=', 1)
        config[parameter.strip()] = value.strip()
  return config




def load_datasets(path):
  """
  Lee el archivo JSON con la definición de los datasets (Batch y Streaming).
  """
  if not os.path.exists(path):
    raise FileNotFoundError(f"No se encontró el archivo de datasets en: {path}")

  with open(path, "r") as f:
    return json.load(f)