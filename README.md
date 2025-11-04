### 📌 Customers–Orders Data Pipeline (Airflow + Postgres + PySpark)

This project implements an end-to-end **Airflow pipeline** that ingests, processes, merges, and analyzes two related datasets using workflow orchestration and containerized deployment.

---

### ✅ What the Pipeline Does

- **Scheduled DAG (@daily)** orchestrated using Airflow.
- **Ingests two datasets**: `customers.csv` and `orders.csv`.
- **Parallel processing** using `TaskGroup` for ingestion and transformation.
- **Data transformations**:
  - Cleaned customer names and formatted dates.
  - Calculated `order_amount = quantity * unit_price`.
- **Merged datasets** on `customer_id`.
- **Loaded final data** into PostgreSQL table `public.customer_orders`.
- **Analysis step**:
  - Used PySpark to compute top customers by spending.
  - Saved visualization to `reports/top_customers.png`.
- **Cleanup task** removes intermediate files after execution.
- **PostgreSQL connection is auto-created via code** (no manual UI setup needed).

---

### 🛠 Technologies Used

| Tool        | Purpose                              |
|-------------|----------------------------------------|
| Airflow     | DAG scheduling and orchestration       |
| Pandas      | Data ingestion & transformation        |
| TaskGroups  | Parallel execution                     |
| PostgreSQL  | Data warehouse                         |
| PySpark     | Analysis & visualization (bonus)       |
| Docker      | Containerized deployment               |

---

### 🗂 DAG Flow
start
└─ ensure_dirs
└─ ingest (customers + orders)
└─ transform
└─ merge_and_load
└─ spark_analysis
└─ cleanup
└─ end

---

### Repository Layout
airflow-pipeline/
├─ docker-compose.yml
├─ Dockerfile
├─ requirements.txt
├─ .env
├─ .gitignore
├─ README.md
├─ .devcontainer/
│  └─ devcontainer.json
├─ dags/
│  └─ pipeline.py
├─ include/
│  ├─ data/
│  │  └─ raw/
│  │     ├─ customers.csv
│  │     └─ orders.csv
│  └─ sql/
│     └─ create_target_tables.sql
├─ jars/
│  └─ postgresql-42.7.4.jar   # (place the JDBC jar here)
└─ reports/