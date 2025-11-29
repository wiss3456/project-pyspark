from pyspark.sql import SparkSession #Importe la classe qui permet de créer la session Spark (entrée centrale pour utiliser Spark).

def extract_customers(spark, path):
    df = spark.read.csv(path, header=True, inferSchema=True) # **`spark.read.csv(...)`** : lecture CSV en DataFrame Spark.
#`header=True` : la première ligne contient les noms de colonnes.
# `inferSchema=True` : Spark tente de deviner les types (int, string, …) automatiquement.
    return df

