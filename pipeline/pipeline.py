from dependencies import start_spark
from jobs.extract import extract_customers
from jobs.transform import clean_customers
from jobs.load import load_to_silver
import time
def run_pipeline():
    spark, log, config = start_spark(app_name="customers_etl")

    raw_path = config["raw_path"]
    silver_path = config["silver_path"]

    log.info("Début du pipeline ETL.")

    df_raw = extract_customers(spark, raw_path)
    df_clean = clean_customers(df_raw)

    load_to_silver(df_clean, silver_path)

    log.info("Pipeline terminé avec succès.")
    time.sleep(999999)
