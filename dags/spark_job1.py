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
    
    print("--- 🥈 PHASE SILVER EN COURS ---")
    
    # 1.1 Customers
    df_customers = spark.read.csv(f"s3a://{bucket}/raw/olist_customers_dataset.csv", header=True, inferSchema=True)
    df_customers_silver = df_customers.dropDuplicates(["customer_id"]).fillna("Unknown")
    df_customers_silver.write.mode("overwrite").option("header", "true").csv(f"s3a://{bucket}/silver/customers/")

    # 1.2 Products
    df_products = spark.read.csv(f"s3a://{bucket}/raw/olist_products_dataset.csv", header=True, inferSchema=True)
    df_products_silver = df_products.dropDuplicates(["product_id"]).fillna({"product_category_name": "divers"})
    df_products_silver.write.mode("overwrite").option("header", "true").csv(f"s3a://{bucket}/silver/products/")

    # 1.3 Orders
    df_orders = spark.read.csv(f"s3a://{bucket}/raw/olist_orders_dataset.csv", header=True, inferSchema=True)
    df_orders_silver = df_orders.withColumn("order_purchase_timestamp", F.to_timestamp("order_purchase_timestamp"))
    df_orders_silver.write.mode("overwrite").option("header", "true").csv(f"s3a://{bucket}/silver/orders/")

    # 1.4 Order Items
    df_items = spark.read.csv(f"s3a://{bucket}/raw/olist_order_items_dataset.csv", header=True, inferSchema=True)
    df_items.write.mode("overwrite").option("header", "true").csv(f"s3a://{bucket}/silver/order_items/")

    print("✅ Données Silver stockées au format csv.")

    
    spark.stop()
if __name__ == "__main__":
    main()