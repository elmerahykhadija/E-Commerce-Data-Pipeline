from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

def main():
    aws_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
    if not aws_key or not aws_secret:
        raise RuntimeError("AWS credentials manquantes")

    spark = (
        SparkSession.builder
        .appName("E-commerce ETL - Raw to Gold")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.access.key", aws_key)
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret)
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
        .getOrCreate()
    )
    
    # Configuration S3
    spark.conf.set("spark.hadoop.fs.s3a.access.key", aws_key)
    spark.conf.set("spark.hadoop.fs.s3a.secret.key", aws_secret)
    spark.conf.set("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    
    bucket = "e-commerce-data-project-pipline"
# ---------------------------------------------------------
    # 2. : PHASE GOLD (Transformation Métier & Star Schema)
    # ---------------------------------------------------------
    print("--- 🥇 PHASE GOLD EN COURS ---")

    # Lecture depuis Silver
    silver_customers = spark.read.csv(f"s3a://{bucket}/silver/customers/", header=True, inferSchema=True)
    silver_products = spark.read.csv(f"s3a://{bucket}/silver/products/", header=True, inferSchema=True)
    silver_orders = spark.read.csv(f"s3a://{bucket}/silver/orders/", header=True, inferSchema=True)
    silver_items = spark.read.csv(f"s3a://{bucket}/silver/order_items/", header=True, inferSchema=True)

    # DIM_CUSTOMERS
    dim_customers = silver_customers.select(
        F.col("customer_id").alias("CUSTOMER_ID"),
        F.col("customer_unique_id").alias("CUSTOMER_UNIQUE_ID"),
        F.col("customer_city").alias("CITY"),
        F.col("customer_state").alias("STATE")
    )

    # DIM_PRODUCTS
    dim_products = silver_products.select(
        F.col("product_id").alias("PRODUCT_ID"),
        F.col("product_category_name").alias("CATEGORY"),
        F.col("product_weight_g").cast("float").alias("WEIGHT_G")
    )

    # FACT_SALES (Jointure Items + Orders)
    fact_sales = silver_items.join(silver_orders, "order_id", "inner") \
        .select(
            F.col("order_id").alias("ORDER_ID"),
            F.col("product_id").alias("PRODUCT_ID"),
            F.col("customer_id").alias("CUSTOMER_ID"),
            F.col("price").cast("float").alias("PRICE"),
            F.col("order_purchase_timestamp").alias("ORDER_DATE")
        )

    # ---------------------------------------------------------
    # : ÉCRITURE FINALE VERS GOLD (CSV pour Snowflake)
    # ---------------------------------------------------------
    print("💾 Exportation des fichiers Gold vers S3...")
    
    # On utilise coalesce(1) pour avoir 1 seul fichier par table (plus simple pour Snowflake COPY INTO)
    dim_customers.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"s3a://{bucket}/gold/dim_customers.csv")
    dim_products.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"s3a://{bucket}/gold/dim_products.csv")
    fact_sales.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"s3a://{bucket}/gold/fact_sales.csv")

    print("🏁 TOUT EST TERMINÉ AVEC SUCCÈS !")
    spark.stop()
if __name__ == "__main__":
    main()