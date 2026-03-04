from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import os

from config import setting


def extract_csv(path):
    df = pd.read_csv(path)
    return df.to_dict(orient="records")  # JSON serializable


def extract_json(path):
    df = pd.read_json(path)
    return df.to_dict(orient="records")  # JSON serializable


def transform_data(**kwargs):
    ti = kwargs["ti"]
    product_df = pd.DataFrame(ti.xcom_pull(task_ids="extract_products"))
    transactions_df = pd.DataFrame(ti.xcom_pull(task_ids="extract_transactions"))
    users_df = pd.DataFrame(ti.xcom_pull(task_ids="extract_users"))

    # handle price
    product_df["price"] = product_df["price"].astype(str)
    product_df["currency"] = product_df["price"].str[:2]
    product_df["price"] = product_df["price"].str[3:].astype(int)

    # handle transaction_date
    transactions_df["transaction_date"] = pd.to_datetime(
        transactions_df["transaction_date"], dayfirst=True, errors="coerce"
    )

    # handle email
    users_df["email"] = users_df["email"].replace("", None)
    users_df["email"] = users_df["email"].where(users_df["email"].notna(), None)

    # merge
    merged_df = transactions_df.merge(product_df, on="product_id", how="left").merge(
        users_df, on="user_id", how="left"
    )
    merged_df["total_transaction"] = merged_df["quantity"] * merged_df["price"]

    # convert Timestamp ke string biar JSON-serializable
    merged_df["transaction_date"] = merged_df["transaction_date"].dt.strftime(
        "%Y-%m-%d"
    )

    return merged_df.to_dict(orient="records")


def save_to_db(**kwargs):
    ti = kwargs["ti"]
    df = pd.DataFrame(ti.xcom_pull(task_ids="transform"))

    engine = create_engine(
        "postgresql+psycopg2://postgres:password@postgres-dwh:5432/data_warehouse"
    )
    df.to_sql("transactions_summary", engine, if_exists="replace", index=False)

    os.makedirs(os.path.dirname(setting.OUTPUT_PATH), exist_ok=True)
    df.to_csv(setting.OUTPUT_PATH, index=False)


with DAG(
    dag_id="etl_pipeline",
    start_date=datetime(2026, 2, 16),
    schedule_interval=None,
    catchup=False,
) as dag:

    extract_products = PythonOperator(
        task_id="extract_products",
        python_callable=extract_csv,
        op_args=[setting.PRODUCTS_PATH],
    )

    extract_transactions = PythonOperator(
        task_id="extract_transactions",
        python_callable=extract_csv,
        op_args=[setting.TRANSACTIONS_PATH],
    )

    extract_users = PythonOperator(
        task_id="extract_users",
        python_callable=extract_json,
        op_args=[setting.USERS_PATH],
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_data,
        provide_context=True,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=save_to_db,
        provide_context=True,
    )

    [extract_products, extract_transactions, extract_users] >> transform >> load
