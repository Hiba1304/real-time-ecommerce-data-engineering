from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

SILVER = Path("/data/silver_stream")
GOLD = Path("/data/gold_airflow")


def check_silver():
    """Vérifie que Silver existe et n'est pas vide."""
    files = list(SILVER.glob("*.parquet"))
    if not files:
        raise FileNotFoundError("Silver est vide ou introuvable")
    print(f"{len(files)} fichiers Silver trouvés")


def build_gold():
    """Calcule le CA par pays à partir de Silver."""
    df = pd.read_parquet(SILVER)
    by_country = (
        df.groupby("Country")
        .agg(total_revenue=("Revenue", "sum"), orders=("InvoiceNo", "nunique"))
        .round(2)
        .reset_index()
        .sort_values("total_revenue", ascending=False)
    )
    GOLD.mkdir(parents=True, exist_ok=True)
    by_country.to_parquet(GOLD / "revenue_by_country.parquet", index=False)
    print(by_country.head())


def quality_checks():
    """Contrôles qualité sur la table Gold."""
    g = pd.read_parquet(GOLD / "revenue_by_country.parquet")
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