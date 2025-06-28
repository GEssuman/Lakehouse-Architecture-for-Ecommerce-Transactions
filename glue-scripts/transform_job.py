import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

import logging

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session


try:
   orders_df = spark.read \
    .format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .load("s3://ecommerce-raw.amalitech-gke/orders/orders_apr_2025.xlsx")
   

   order_items_df = spark.read \
    .format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .load("s3://ecommerce-raw.amalitech-gke/order-items/order_items_apr_2025.xlsx")
   
   products_df = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load("s3://ecommerce-raw.amalitech-gke/products/products.csv")
    
   products_df.show()
    # pass
    

except Exception as e:
    print(f"Error during transformation: {e}")
    
