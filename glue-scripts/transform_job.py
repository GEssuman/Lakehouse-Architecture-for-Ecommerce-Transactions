import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
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
    
   # Paths to Delta tables
   DEPARTMENTS_PATH = "s3://vehicle-rental-delta.amalitech-gke/delta/departments"
   PRODUCTS_PATH = "s3://vehicle-rental-delta.amalitech-gke/delta/products"
   ORDERS_PATH = "s3://vehicle-rental-delta.amalitech-gke/delta/orders"
   ORDER_ITEMS_PATH = "s3://vehicle-rental-delta.amalitech-gke/delta/order_items"

   products_df = products_df.dropDuplicates(["product_id"])


   department_df = products_df.select("department").distinct()
   try:
       existing_department_df = spark.read.format("delta").load(DEPARTMENTS_PATH)
   except:
       window_spec = Window.orderBy("department")
    # Add sequential department_id
       existing_department_df = department_df.withColumn(
        "department_id",
        F.row_number().over(window_spec)
    )
       
       existing_department_df.write.format("delta").mode("overwrite").save(DEPARTMENTS_PATH)

#    # Find new departments (not in existing)
   new_departments_df = department_df.join(existing_department_df, on="department", how="left_anti")

   new_departments_df.write.format("delta").mode("append").save(DEPARTMENTS_PATH)

   product_df = products_df.withColumn("product_name", 
                            F.element_at(F.split(F.col("product_name"), "_"), -1)).drop("department_id")
   
   product_with_new_ids_df = product_df.join(existing_department_df, on="department", how="left").drop("department")
   product_with_new_ids_df.show()
   product_with_new_ids_df = product_with_new_ids_df.select(
       F.col("product_id").cast("string").alias("product_id"),
       F.col("department_id").cast("int").alias("department_id"),
       F.col("product_name").cast("string").alias("product_name")
   )

#    product_with_new_ids_df.show()
#    print(products_df.count())


   orders_df = orders_df.drop("date")
   orders_df =  orders_df.select(
       F.col("order_num").cast("int").alias("order_num"),
       F.col("order_id").cast("int").alias("order_id"),
       F.col("user_id").cast("int").alias("user_id"),
       F.col("total_amount").cast("double").alias("total_amount"),
       F.col("order_timestamp").cast("int").alias("order_timestamp")
   )

   order_items_df.drop("date")
   order_items_df = order_items_df.select(
       F.col("id").cast("int").alias("id"),
       F.col("order_id").cast("int").alias("order_id"),
       F.col("days_since_prior_order").cast("int").alias("days_since_prior_order"),
       F.col("product_id").cast("int").alias("product_id"),
       F.col("add_to_cart_order").cast("int").alias("add_to_cart_order"),
       F.col("reordered").cast("boolean").alias("reodered"),
       F.col("order_timestamp").cast("timestamp").alias("order_timestamp")
   )

   fk_violations = order_items_df.join(product_with_new_ids_df, on="product_id", how="left_anti")
   if fk_violations.count() > 0:
      raise ValueError("Foreign key violation: Some product_id in order_items do not exist in products")





   
   product_with_new_ids_df.write.format("delta").mode("overwrite").save(PRODUCTS_PATH)
   

   orders_df.write.format("delta").mode("overwrite").save(ORDERS_PATH)
   

   order_items_df.write.format("delta").mode("overwrite").save(ORDER_ITEMS_PATH)
   
except Exception as e:
    print(f"Error during transformation: {e}")
    
