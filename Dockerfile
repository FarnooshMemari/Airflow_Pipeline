FROM apache/airflow:2.9.3-python3.11

# System deps for Spark JDBC
USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless curl && \
    rm -rf /var/lib/apt/lists/*

# PostgreSQL JDBC driver (used by PySpark JDBC)
RUN mkdir -p /opt/airflow/jars && \
    curl -L -o /opt/airflow/jars/postgresql-42.7.4.jar https://jdbc.postgresql.org/download/postgresql-42.7.4.jar

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
