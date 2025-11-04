from datetime import datetime
from pathlib import Path
import os
import pandas as pd

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup

BASE_DIR = Path("/opt/airflow")
BASE_DIR = Path("/opt/airflow")
RAW_DIR = BASE_DIR / "include" / "data" / "raw"
STAGE_DIR = BASE_DIR / "include" / "data" / "stage"
REPORTS_DIR = BASE_DIR / "reports"
JDBC_JAR = "/opt/airflow/jars/postgresql-42.7.4.jar"

WARE_CONN_ID = "warehouse_postgres"  # we'll define in code using env vars


def _ensure_dirs():
    for p in [STAGE_DIR, REPORTS_DIR]:
        p.mkdir(parents=True, exist_ok=True)


def _ingest_file(src_name: str, dest_key: str, **context):
    run_tag = context["ts_nodash"]  # unique per run, e.g. 20251104T073717
    src = RAW_DIR / src_name
    dest = STAGE_DIR / f"{Path(src_name).stem}_{run_tag}.csv"
    df = pd.read_csv(src)
    df.to_csv(dest, index=False)

    # staged path for downstream
    context["ti"].xcom_push(key=dest_key, value=str(dest))
    # also share the run_tag so cleanup can target only this run's files
    context["ti"].xcom_push(key="run_tag", value=run_tag)


def _transform_customers(**context):
    path = context["ti"].xcom_pull(key="customers_path")
    df = pd.read_csv(path)
    df["customer_name"] = df["customer_name"].str.title()
    df["signup_date"] = pd.to_datetime(df["signup_date"]).dt.date
    out = Path(path).with_name(Path(path).stem + "_clean.csv")
    df.to_csv(out, index=False)
    context["ti"].xcom_push(key="customers_clean", value=str(out))


def _transform_orders(**context):
    path = context["ti"].xcom_pull(key="orders_path")
    df = pd.read_csv(path)
    df["order_date"] = pd.to_datetime(df["order_date"]).dt.date
    df["order_amount"] = (df["quantity"] * df["unit_price"]).round(2)
    out = Path(path).with_name(Path(path).stem + "_clean.csv")
    df.to_csv(out, index=False)
    context["ti"].xcom_push(key="orders_clean", value=str(out))


def _merge_and_load(**context):
    from sqlalchemy import text, create_engine
    from sqlalchemy.types import Date, Integer, Numeric, Text
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    ti = context["ti"]
    cust_p = ti.xcom_pull(key="customers_clean")
    ord_p = ti.xcom_pull(key="orders_clean")
    for label, p in [("customers_clean", cust_p), ("orders_clean", ord_p)]:
        if not p or not Path(p).exists():
            raise FileNotFoundError(
                f"Upstream file for {label} is missing: {p}. "
                "Clear the transform TaskGroup (with downstream) or trigger a fresh DAG run."
            )
    customers = pd.read_csv(cust_p)
    orders = pd.read_csv(ord_p)
    customers["signup_date"] = pd.to_datetime(customers["signup_date"]).dt.date
    orders["order_date"] = pd.to_datetime(orders["order_date"]).dt.date
    orders["order_amount"] = pd.to_numeric(orders["order_amount"])
    merged = orders.merge(customers, on="customer_id", how="left")
    hook = PostgresHook(postgres_conn_id="warehouse_postgres")
    engine = hook.get_sqlalchemy_engine()
    create_sql = """
    CREATE TABLE IF NOT EXISTS public.customer_orders (
        customer_id INT,
        customer_name TEXT,
        signup_date DATE,
        order_id INT,
        order_date DATE,
        item TEXT,
        quantity INT,
        unit_price NUMERIC(10,2),
        order_amount NUMERIC(12,2)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(create_sql))
    from io import StringIO

    pg_conn = hook.get_conn()
    cur = pg_conn.cursor()
    buf = StringIO()
    copy_cols = [
        "customer_id",
        "customer_name",
        "signup_date",
        "order_id",
        "order_date",
        "item",
        "quantity",
        "unit_price",
        "order_amount",
    ]
    merged = merged.loc[:, copy_cols]
    merged.to_csv(buf, index=False, header=True)
    buf.seek(0)
    copy_sql = (
        "COPY public.customer_orders (customer_id, customer_name, signup_date, order_id, order_date, item, quantity, unit_price, order_amount)"
        " FROM STDIN WITH CSV HEADER"
    )
    try:
        cur.copy_expert(copy_sql, buf)
        pg_conn.commit()
    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            pg_conn.close()
        except Exception:
            pass


def _spark_analysis(**context):
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import sum as _sum, col
    import matplotlib.pyplot as plt

    # Read DB params from env (docker-compose) to avoid hardcoding
    host = os.environ.get("WAREHOUSE_HOST", "warehouse-db")
    db = os.environ.get("WAREHOUSE_DB", "warehouse")
    user = os.environ.get("WAREHOUSE_USER", "ware")
    pw = os.environ.get("WAREHOUSE_PASSWORD", "ware")
    url = f"jdbc:postgresql://{host}:5432/{db}"

    # If a Spark session already exists, stop it so our jar/classpath configs take effect
    existing = SparkSession.getActiveSession()
    if existing:
        try:
            existing.stop()
        except Exception:
            pass

    spark = (
        SparkSession.builder.appName("airflow-pyspark-analysis")
        .master("local[*]")
        .config("spark.jars", JDBC_JAR)
        # Ensure the JDBC jar is on the driver and executor classpaths so Spark can load the driver
        .config("spark.driver.extraClassPath", JDBC_JAR)
        .config("spark.executor.extraClassPath", JDBC_JAR)
        .getOrCreate()
    )

    # Add the jar to the running SparkContext as well (covers cases where classpath wasn't picked up)
    try:
        spark.sparkContext.addJar(JDBC_JAR)
    except Exception:
        pass
    try:
        # also add via the JavaSparkContext for extra robustness
        spark._jsc.addJar(JDBC_JAR)
    except Exception:
        pass

    # Try loading via Spark JDBC; if the driver class isn't available, fall back
    # to reading via psycopg2 into pandas and converting to a Spark DataFrame.
    try:
        df = (
            spark.read.format("jdbc")
            .option("url", url)
            .option("driver", "org.postgresql.Driver")
            .option("dbtable", "public.customer_orders")
            .option("user", user)
            .option("password", pw)
            .load()
        )
        # Trigger a small action to force evaluation and catch deferred errors
        _ = df.limit(1).count()
        used_fallback = False
    except Exception:
        # Fallback: read via psycopg2 (PostgresHook) into pandas, then convert
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id="warehouse_postgres")
        pg_conn = hook.get_conn()
        try:
            pdf = pd.read_sql_query("SELECT * FROM public.customer_orders", con=pg_conn)
        finally:
            try:
                pg_conn.close()
            except Exception:
                pass

        df = spark.createDataFrame(pdf)
        used_fallback = True

    top = (
        df.groupBy("customer_id", "customer_name")
        .agg(_sum(col("order_amount")).alias("total_spend"))
        .orderBy(col("total_spend").desc())
        .limit(10)
        .toPandas()
    )

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    out_png = REPORTS_DIR / "top_customers.png"

    # Simple bar plot (requirement says: a visualization)
    plt.figure(figsize=(6, 4))
    plt.bar(top["customer_name"], top["total_spend"])
    plt.xticks(rotation=30, ha="right")
    plt.ylabel("Total Spend ($)")
    plt.title("Top Customers by Total Spend")
    plt.tight_layout()
    plt.savefig(out_png)
    plt.close()

    spark.stop()


def _cleanup(**context):
    run_tag = context["ti"].xcom_pull(key="run_tag")
    if not run_tag:
        return
    for p in STAGE_DIR.glob(f"*{run_tag}*.csv"):
        try:
            p.unlink()
        except Exception:
            pass


with DAG(
    dag_id="customers_orders_pipeline",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args={"owner": "you"},
    tags=["assignment", "pyspark", "postgres"],
) as dag:

    start = EmptyOperator(task_id="start")

    ensure_dirs = PythonOperator(
        task_id="ensure_dirs",
        python_callable=_ensure_dirs,
    )

    # Ingest in parallel
    with TaskGroup(group_id="ingest") as ingest:
        ingest_customers = PythonOperator(
            task_id="ingest_customers",
            python_callable=_ingest_file,
            op_kwargs={"src_name": "customers.csv", "dest_key": "customers_path"},
        )
        ingest_orders = PythonOperator(
            task_id="ingest_orders",
            python_callable=_ingest_file,
            op_kwargs={"src_name": "orders.csv", "dest_key": "orders_path"},
        )

    # Transform in parallel
    with TaskGroup(group_id="transform") as transform:
        t_customers = PythonOperator(
            task_id="transform_customers",
            python_callable=_transform_customers,
        )
        t_orders = PythonOperator(
            task_id="transform_orders",
            python_callable=_transform_orders,
        )

        # parallel inside the group
        [t_customers, t_orders]

    merge_load = PythonOperator(
        task_id="merge_and_load",
        python_callable=_merge_and_load,
    )

    spark_analysis = PythonOperator(
        task_id="spark_analysis",
        python_callable=_spark_analysis,
    )

    cleanup = PythonOperator(
        task_id="cleanup_intermediate_files",
        python_callable=_cleanup,
        trigger_rule="all_done",
    )

    end = EmptyOperator(task_id="end")

    chain(
        start,
        ensure_dirs,
        ingest,
        transform,
        merge_load,
        spark_analysis,
        cleanup,
        end,
    )

from airflow.models import Connection
from airflow.settings import Session


def _ensure_connection():
    conn_id = "warehouse_postgres"
    session = Session()
    exists = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    if not exists:
        conn = Connection(
            conn_id=conn_id,
            conn_type="postgres",
            host=os.environ.get("WAREHOUSE_HOST", "warehouse-db"),
            login=os.environ.get("WAREHOUSE_USER", "ware"),
            password=os.environ.get("WAREHOUSE_PASSWORD", "ware"),
            schema=os.environ.get("WAREHOUSE_DB", "warehouse"),
            port=5432,
        )
        session.add(conn)
        session.commit()
    session.close()


_ensure_connection()
