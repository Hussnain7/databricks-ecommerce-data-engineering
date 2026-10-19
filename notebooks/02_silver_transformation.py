# Databricks notebook source
# DBTITLE 1,transformation
from pyspark.sql.functions import col,trim,to_date

customers = spark.table("workspace.bronze.customers")
products = spark.table("workspace.bronze.products")
orders = spark.table("workspace.bronze.orders")
order_items = spark.table("workspace.bronze.order_items")

customers.groupBy("customer_id") \
    .count() \
    .filter("count > 1") \
    .show()

customers.filter(
    customers.customer_id.isNull() |
    customers.customer_name.isNull() |
    customers.country.isNull()
).show()
    
    

customers_silver = customers.select(
    "customer_id",
    trim("customer_name").alias("customer_name"),
    trim("country").alias("country")
)
customers_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.silver.customers")

customers.show()
customers.printSchema()
print("customers:", customers.count())

products.groupBy("product_id") \
    .count() \
    .filter("count > 1") \
    .show()
products.filter(
    products.product_id.isNull() |
    products.product_name.isNull() |
    products.category.isNull() |
    products.price.isNull()
).show()

products_silver = products.select(
    "product_id",
    trim("product_name").alias("product_name"),
    trim("category").alias("category"),
)
products_silver.filter(
    col("price") < 0
).show()
products_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.silver.products")
products.show()
products.printSchema()
print("products:", products.count())

orders.filter(
    orders.order_id.isNull() |
    orders.customer_id.isNull() |
    orders.order_date.isNull() |
    orders.status.isNull()
).show()


print("Duplicate order IDs:")
orders.groupBy("order_id") \
    .count() \
    .filter("count > 1") \
    .show()


print("Duplicate complete order rows:")
orders.groupBy(
    "order_id",
    "customer_id",
    "order_date",
    "status"
).count().filter("count > 1").show()
orders.join(
    customers,
    orders.customer_id == customers.customer_id,
    "left"
).filter(
    customers.customer_id.isNull()
).select(
    orders.order_id,
    orders.customer_id
).show()


orders_silver = orders.select(
    col("order_id"),
    col("customer_id"),
    to_date(col("order_date")).alias("order_date"),
    col("status"),
)
orders_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.silver.orders")

orders.show()
orders.printSchema()
print("orders:", orders.count())


order_items.groupBy("order_item_id") \
    .count() \
    .filter("count > 1") \
    .show()

order_items.filter(
    order_items.order_item_id.isNull() |
    order_items.order_id.isNull() |
    order_items.product_id.isNull() |
    order_items.quantity.isNull()
).show()

print("Order items with no matching order:")
order_items.join(
    orders,
    order_items.order_id == orders.order_id,
    "left"
).filter(
    orders.order_id.isNull()
).select(
    order_items.order_item_id,
    order_items.order_id
).show()

print("Order items with no matching product:")
order_items.join(
    products,
    order_items.product_id == products.product_id,
    "left"
).filter(
    products.product_id.isNull()
).select(
    order_items.order_item_id,
    order_items.product_id
).show()

order_items_silver = order_items.select(
    col("order_item_id"),
    col("order_id"),
    col("product_id"),
    col("quantity"),
)
order_items_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("workspace.silver.order_items")

order_items.show()
order_items.printSchema()
print("order_items:", order_items.count())