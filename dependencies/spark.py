"""
Ce module gère la création propre d'une SparkSession pour tout le projet ETL.
"""

import json
import os  # ← Ajouter cet import
from pyspark.sql import SparkSession
from dependencies import get_logger

# Configuration pour Windows - À ajouter ici ↓
os.environ['HADOOP_HOME'] = r'C:\hadoop'

def start_spark(app_name="my_etl", config_path="configs/etl_config.json"):
    """
    Crée une SparkSession + charge la configuration + crée un logger.
    Retourne: spark, log, config
    """
    log=get_logger(name="etl_logger")

    # 2. Créer la SparkSession
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.warehouse.dir", "file:///C:/tmp/spark-warehouse")  # ← Ajouter
        .config("spark.driver.extraJavaOptions", "-Djava.io.tmpdir=C:/tmp")   # ← Ajouter
        .config("spark.executor.extraJavaOptions", "-Djava.io.tmpdir=C:/tmp") # ← Ajouter
        .getOrCreate()
    )
    log.info("SparkSession créée avec succès.")

    # 3. Charger la config
    with open(config_path) as f:
        config = json.load(f)
    log.info("Configuration chargée.")

    return spark, log, config