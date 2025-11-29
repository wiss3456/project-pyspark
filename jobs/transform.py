from pyspark.sql.functions import col, trim, lower #col() référence une colonne, trim() supprime espaces, lower() met en minuscules.

def clean_customers(df):
    df = df.withColumn("name", trim(col("name"))) #withColumn : remplace ou crée une colonne. Ici on remplace name par sa version sans espaces externes.
    df = df.withColumn("city", lower(col("city"))) #Met la colonne `city` en minuscules pour avoir une donnée normalisée (`Casa` → `casa`).
    df = df.filter(col("age") > 0) # Filtre les lignes où age > 0 ; élimine valeurs invalides ou négatives.
    return df

