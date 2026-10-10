import os
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

# Chemins Azure Data Lake (conteneur/dossier). La clé vient du .env.
SILVER = "silver/silver_stream"
GOLD_DIR = "gold/gold_airflow"          # séparé du Gold Spark pour pouvoir comparer
GOLD_FILE = f"abfs://{GOLD_DIR}/revenue_by_country.parquet"


def _storage_options():
    """Identifiants Azure lus dans les variables d'environnement (jamais dans le code)."""
    return {
        "account_name": os.environ["AZURE_STORAGE_ACCOUNT"],
        "account_key": os.environ["AZURE_STORAGE_KEY"],
    }


def check_silver():
    """Vérifie que Silver existe sur Azure et n'est pas vide."""
    from adlfs import AzureBlobFileSystem

    fs = AzureBlobFileSystem(**_storage_options())
    files = fs.glob(f"{SILVER}/*.parquet")
    if not files:
        raise FileNotFoundError("Silver est vide ou introuvable sur Azure")
    print(f"{len(files)} fichiers Silver trouvés sur Azure")


def build_gold():
    """Calcule le CA par pays à partir de Silver (Azure) et écrit Gold sur Azure."""
    opts = _storage_options()
    df = pd.read_parquet(f"abfs://{SILVER}", storage_options=opts)
    by_country = (
        df.groupby("Country")
        .agg(total_revenue=("Revenue", "sum"), orders=("InvoiceNo", "nunique"))
        .round(2)
        .reset_index()
        .sort_values("total_revenue", ascending=False)
    )
    by_country.to_parquet(GOLD_FILE, index=False, storage_options=opts)
    print(by_country.head())


def quality_checks():
    """Contrôles qualité sur la table Gold (lue depuis Azure)."""
    g = pd.read_parquet(GOLD_FILE, storage_options=_storage_options())
    assert g["Country"].notna().all(), "Pays manquants"
    assert g["Country"].is_unique, "Pays en double"
    assert (g["total_revenue"] > 0).all(), "CA négatif ou nul"
    print("Contrôles qualité OK :", len(g), "pays")


def notify():
    print("Pipeline terminé avec succès")


with DAG(
    dag_id="sales_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,          # déclenchement manuel (mettre "@daily" plus tard)
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=1)},
) as dag:
    t1 = PythonOperator(task_id="check_silver", python_callable=check_silver)
    t2 = PythonOperator(task_id="build_gold", python_callable=build_gold)
    t3 = PythonOperator(task_id="quality_checks", python_callable=quality_checks)
    t4 = PythonOperator(task_id="notify", python_callable=notify)

    t1 >> t2 >> t3 >> t4