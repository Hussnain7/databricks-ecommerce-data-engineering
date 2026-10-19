# Databricks notebook source
# COMMAND ----------
# DAY 1: Read the four raw CSV files.
# IMPORTANT: After uploading the CSV files to Databricks, replace the paths below
# with the actual paths shown by your Databricks workspace.

customers_path = "/Volumes/YOUR_CATALOG/YOUR_SCHEMA/YOUR_VOLUME/customers.csv"
products_path = "/Volumes/YOUR_CATALOG/YOUR_SCHEMA/YOUR_VOLUME/products.csv"
orders_path = "/Volumes/YOUR_CATALOG/YOUR_SCHEMA/YOUR_VOLUME/orders.csv"
order_items_path = "/Volumes/YOUR_CATALOG/YOUR_SCHEMA/YOUR_VOLUME/order_items.csv"

# COMMAND ----------
customers = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(customers_path)
)

products = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(products_path)
)

orders = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(orders_path)
)

order_items = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(order_items_path)
)

# COMMAND ----------
customers.show()
customers.printSchema()
print("customers:", customers.count())

products.show()
products.printSchema()
print("products:", products.count())

orders.show()
orders.printSchema()
print("orders:", orders.count())

order_items.show()
order_items.printSchema()
print("order_items:", order_items.count())
