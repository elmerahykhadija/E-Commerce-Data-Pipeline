import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import os
from dotenv import load_dotenv
import shutil

from airflow.operators.bash import BashOperator
load_dotenv()

# ===== FONCTIONS =====

def upload_local_to_s3():
    """Upload des fichiers CSV locaux vers S3"""
    
    # Chemin local où vous avez les données Kaggle
    local_data_path = "/opt/airflow/dags/data"  
    
    if not os.path.exists(local_data_path):
        raise FileNotFoundError(f"Le dossier {local_data_path} n'existe pas!")
    
    print(f"📁 Lecture des fichiers depuis: {local_data_path}")
    
    # Connexion S3
    s3_hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = 'e-commerce-data-project-pipline'
    
    # Upload vers S3
    uploaded_count = 0
    for filename in os.listdir(local_data_path):
        if filename.endswith('.csv'):
            local_file = os.path.join(local_data_path, filename)
            s3_key = f"raw/{filename}"
            
            s3_hook.load_file(
                filename=local_file,
                key=s3_key,
                bucket_name=bucket_name,
                replace=True
            )
            print(f"✅ {filename} → s3://{bucket_name}/{s3_key}")
            uploaded_count += 1
    
    if uploaded_count == 0:
        raise ValueError(f"Aucun fichier CSV trouvé dans {local_data_path}")
    
    print(f"✅ Total: {uploaded_count} fichiers uploadés vers S3")


default_args = {
    'owner': 'khadija',
    'start_date': datetime(2026, 3, 7)
}

dag = DAG(
    dag_id="etl_oltp_to_olap",
    default_args=default_args,
    schedule='@daily'
)
# ===== TÂCHES =====
raw_to_silver = BashOperator(
    task_id='raw_to_silver',
    bash_command="""
    set -e
    export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
    export PATH=$JAVA_HOME/bin:$PATH
    spark-submit --master local \
      --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
      /opt/airflow/dags/spark_job1.py
    """,
    dag=dag
)
silver_to_gold=BashOperator(
    task_id='silver_to_gold',
    bash_command="""
    set -e
    export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
    export PATH=$JAVA_HOME/bin:$PATH
    spark-submit --master local \
      --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        /opt/airflow/dags/spark_job2.py
    """,
    dag=dag
)
# 1. Téléchargement Local → S3
download_task = PythonOperator(
    task_id="download_local_to_s3",
    python_callable=upload_local_to_s3,
    dag=dag
)

# 4. Charger DIM_CUSTOMERS
load_dim_customers = SQLExecuteQueryOperator(
    task_id='load_dim_customers',
    conn_id='snowflake_id',
    sql="""
        USE SCHEMA E_COMMERCE_DB.GOLD;
        TRUNCATE TABLE DIM_CUSTOMERS;
        COPY INTO DIM_CUSTOMERS
        FROM @my_s3_gold_stage/dim_customers.csv
        FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)
        FORCE = TRUE;
    """,
    dag=dag
)

# 5. Charger DIM_PRODUCTS
load_dim_products = SQLExecuteQueryOperator(
    task_id='load_dim_products',
    conn_id='snowflake_id',
    sql="""
        USE SCHEMA E_COMMERCE_DB.GOLD;
        TRUNCATE TABLE DIM_PRODUCTS;
        COPY INTO DIM_PRODUCTS
        FROM @my_s3_gold_stage/dim_products.csv
        FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)
        FORCE = TRUE;
    """,
    dag=dag
)

# 6. Charger FACT_SALES
load_fact_sales = SQLExecuteQueryOperator(
    task_id='load_fact_sales',
    conn_id='snowflake_id',
    sql="""
        USE SCHEMA E_COMMERCE_DB.GOLD;
        TRUNCATE TABLE FACT_SALES;
        COPY INTO FACT_SALES
        FROM @my_s3_gold_stage/fact_sales.csv
        FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1)
        FORCE = TRUE;
    """,
    dag=dag
)

# ===== ORDRE D'EXÉCUTION =====
download_task >> raw_to_silver >> silver_to_gold >> [load_dim_customers, load_dim_products, load_fact_sales]