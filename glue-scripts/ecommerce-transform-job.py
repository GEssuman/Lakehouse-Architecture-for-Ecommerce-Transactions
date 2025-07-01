import sys
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import boto3
from datetime import datetime

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Accept job parameters from Step Function
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'bucket', 'key'])
input_bucket = args['bucket']
input_key = args['key']
input_path = f"s3://{input_bucket}/{input_key}"

# Define Delta output paths
BASE_DELTA_PATH = "s3://ecommerce-delta.amalitech-gke/delta"
DEPARTMENTS_PATH = f"{BASE_DELTA_PATH}/departments"
PRODUCTS_PATH = f"{BASE_DELTA_PATH}/products"
ORDERS_PATH = f"{BASE_DELTA_PATH}/orders"
ORDER_ITEMS_PATH = f"{BASE_DELTA_PATH}/order_items"

try:
    
    if "products" in input_key:
        print("Processing PRODUCTS file")

        products_df = spark.read \
            .format("csv") \
            .option("header", "true") \
            .load(input_path) \
            .dropDuplicates(["product_id"])

        department_df = products_df.select("department").distinct()

        try:
            existing_department_df = spark.read.format("delta").load(DEPARTMENTS_PATH)
        except:
            window_spec = Window.orderBy("department")
            existing_department_df = department_df.withColumn(
                "department_id",
                F.row_number().over(window_spec)
            )
            existing_department_df.write.format("delta").mode("overwrite").save(DEPARTMENTS_PATH)

        new_departments_df = department_df.join(existing_department_df, on="department", how="left_anti")
        if new_departments_df.count() > 0:
            new_departments_df.write.format("delta").mode("append").save(DEPARTMENTS_PATH)

        product_df = products_df.withColumn(
            "product_name", F.element_at(F.split(F.col("product_name"), "_"), -1)
        ).drop("department_id")

        product_with_new_ids_df = product_df.join(existing_department_df, on="department", how="left").drop("department")

        product_with_new_ids_df = product_with_new_ids_df.select(
            F.col("product_id").cast("int"),
            F.col("department_id").cast("int"),
            F.col("product_name").cast("string")
        )

        product_with_new_ids_df.write.format("delta").mode("overwrite").save(PRODUCTS_PATH)

    elif "orders" in input_key:
        print("Processing ORDERS file")

        orders_df = spark.read \
            .format("com.crealytics.spark.excel") \
            .option("header", "true") \
            .load(input_path) \
            .drop("date")

        orders_df = orders_df.select(
            F.col("order_num").cast("int"),
            F.col("order_id").cast("int"),
            F.col("user_id").cast("int"),
            F.col("total_amount").cast("double"),
            F.col("order_timestamp").cast("int")
        )

        orders_df.write.format("delta").mode("overwrite").save(ORDERS_PATH)

    elif "order-items" in input_key or "order_items" in input_key:
        print("Processing ORDER_ITEMS file")

        order_items_df = spark.read \
            .format("com.crealytics.spark.excel") \
            .option("header", "true") \
            .load(input_path) \
            .drop("date")

        order_items_df = order_items_df.select(
            F.col("id").cast("int"),
            F.col("order_id").cast("int"),
            F.col("days_since_prior_order").cast("int"),
            F.col("product_id").cast("int"),
            F.col("add_to_cart_order").cast("int"),
            F.col("reordered").cast("boolean").alias("reordered"),
            F.col("order_timestamp").cast("timestamp")
        )

        # FK validation: ensure product exists
        products_df = spark.read.format("delta").load(PRODUCTS_PATH)
        fk_violations = order_items_df.join(products_df, on="product_id", how="left_anti")
        if fk_violations.count() > 0:
            raise ValueError("Foreign key violation: product_id in order_items not found in products table")

        order_items_df.write.format("delta").mode("overwrite").save(ORDER_ITEMS_PATH)

    else:
        raise ValueError("Unknown file type. Cannot process this input.")
    
    s3 = boto3.client("s3")

    # Define archive path (prefix 'archive/')
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    archive_key = f"archive_{timestamp_str}/{input_key}"

    # Copy the object
    s3.copy_object(
        Bucket="ecommerce-archive.amalitch-gke",
        CopySource={'Bucket': input_bucket, 'Key': input_key},
        Key=archive_key
    )

    # Delete the original object
    s3.delete_object(Bucket=input_bucket, Key=input_key)

except Exception as e:
    print(f"Error during transformation: {e}")
    raise
