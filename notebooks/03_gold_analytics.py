# Databricks notebook source
# DBTITLE 1,analytics
customers= spark.table("workspace.silver.customers")
products= spark.table("workspace.silver.products")
orders= spark.table("workspace.silver.orders")
order_items = spark.table("workspace.silver.order_items")

from pyspark.sql.functions import col, count, sum as spark_sum, when, desc, month, date_format, countDistinct

# Join orders + order_items + customers to get per-customer order summary
joined = orders.join(order_items, "order_id", "left") \
    .join(customers, "customer_id", "left")
joined.show()
customer_order_summary = joined.groupBy("customer_id", "customer_name", "country") \
    .agg(
        countDistinct("order_id").alias("total_orders"),
        countDistinct(when(col("status") == "Completed", col("order_id")).otherwise(None)).alias("completed_orders"),
        countDistinct(when(col("status") == "Cancelled", col("order_id")).otherwise(None)).alias("cancelled_orders"),
        spark_sum("quantity").alias("total_quantity_ordered")
    ) \
    .orderBy(desc("total_quantity_ordered"))

customer_order_summary.write.mode("overwrite").saveAsTable("workspace.gold.customer_order_summary")
display(spark.table("workspace.gold.customer_order_summary"))

# Join order_items + products + orders (only completed orders for sales) to get per-product performance
completed_items = order_items.join(orders, "order_id") \
    .filter(col("status") == "Completed") \
    .join(products, "product_id")
completed_items.show()

product_performance = completed_items.groupBy("product_id", "product_name", "category") \
    .agg(
        spark_sum("quantity").alias("total_quantity_sold"),
        countDistinct("order_id").alias("orders_containing_product")
    ) \
    .orderBy(desc("total_quantity_sold"))

product_performance.write.mode("overwrite").saveAsTable("workspace.gold.product_performance")
display(spark.table("workspace.gold.product_performance"))

# Aggregate product performance up to category level
category_performance = completed_items.groupBy("category") \
    .agg(
        spark_sum("quantity").alias("total_quantity_sold"),
        countDistinct("order_id").alias("total_orders"),
        countDistinct("product_id").alias("unique_products_sold")
    ) \
    .orderBy(desc("total_quantity_sold"))

category_performance.write.mode("overwrite").saveAsTable("workspace.gold.category_performance")
display(spark.table("workspace.gold.category_performance"))

# Monthly breakdown of orders by status
monthly_trends = orders.withColumn("order_month", date_format(col("order_date"), "yyyy-MM")) \
    .groupBy("order_month") \
    .agg(
        count("*").alias("total_orders"),
        spark_sum(when(col("status") == "Completed", 1).otherwise(0)).alias("completed"),
        spark_sum(when(col("status") == "Cancelled", 1).otherwise(0)).alias("cancelled")
    ) \
    .orderBy("order_month")

monthly_trends.write.mode("overwrite").saveAsTable("workspace.gold.monthly_order_trends")
display(spark.table("workspace.gold.monthly_order_trends"))


# Country-level summary: customers, orders, and total quantity from completed orders
country_joined = orders.join(order_items, "order_id", "left") \
    .join(customers, "customer_id") 

country_sales = country_joined.groupBy("country") \
    .agg(
        countDistinct("customer_id").alias("total_customers"),
        countDistinct("order_id").alias("total_orders"),
        spark_sum(when(col("status") == "Completed", col("quantity")).otherwise(0)).alias("total_quantity_sold")
    ) \
    .orderBy(desc("total_quantity_sold"))

country_sales.write.mode("overwrite").saveAsTable("workspace.gold.country_sales_summary")
display(spark.table("workspace.gold.country_sales_summary"))

# Overall business KPIs — a single-row summary
completed_orders = orders.filter(col("status") == "Completed")
completed_order_ids = completed_orders.select("order_id")
completed_qty = order_items.join(completed_order_ids, "order_id").agg(spark_sum("quantity").alias("q")).collect()[0]["q"] or 0

kpis = spark.sql(f"""
SELECT
  (SELECT count(*) FROM workspace.silver.customers) AS total_customers,
  (SELECT count(*) FROM workspace.silver.products) AS total_products,
  (SELECT count(*) FROM workspace.silver.orders) AS total_orders,
  (SELECT count(*) FROM workspace.silver.orders WHERE status = 'Completed') AS completed_orders,
  (SELECT count(*) FROM workspace.silver.orders WHERE status = 'Cancelled') AS cancelled_orders,
  {completed_qty} AS total_quantity_sold,
  ROUND(
    (SELECT count(*) FROM workspace.silver.orders WHERE status = 'Completed') * 100.0 /
    (SELECT count(*) FROM workspace.silver.orders), 1
  ) AS order_completion_rate_pct
""")

kpis.write.mode("overwrite").saveAsTable("workspace.gold.business_kpis")
display(spark.table("workspace.gold.business_kpis"))



# COMMAND ----------

# DBTITLE 1,Title
# MAGIC %md
# MAGIC # Gold Layer — Business Analytics
# MAGIC
# MAGIC This notebook joins the four silver tables (`customers`, `products`, `orders`, `order_items`) to produce business-ready gold-layer analytics: customer order summaries, product & category performance, monthly order trends, country-level sales, and overall KPIs. All metrics are quantity-based (no price column exists in the source data).

# COMMAND ----------

# DBTITLE 1,Customer Order Summary
from pyspark.sql.functions import col, count, sum as spark_sum, when, desc, month, date_format, countDistinct

# Join orders + order_items + customers to get per-customer order summary
joined = orders.join(order_items, "order_id", "left") \
    .join(customers, "customer_id", "left")
joined.show()
customer_order_summary = joined.groupBy("customer_id", "customer_name", "country") \
    .agg(
        countDistinct("order_id").alias("total_orders"),
        countDistinct(when(col("status") == "Completed", col("order_id")).otherwise(None)).alias("completed_orders"),
        countDistinct(when(col("status") == "Cancelled", col("order_id")).otherwise(None)).alias("cancelled_orders"),
        spark_sum("quantity").alias("total_quantity_ordered")
    ) \
    .orderBy(desc("total_quantity_ordered"))

customer_order_summary.write.mode("overwrite").saveAsTable("workspace.gold.customer_order_summary")
display(spark.table("workspace.gold.customer_order_summary"))

# COMMAND ----------

# DBTITLE 1,Product Performance
# Join order_items + products + orders (only completed orders for sales) to get per-product performance
completed_items = order_items.join(orders, "order_id") \
    .filter(col("status") == "Completed") \
    .join(products, "product_id")
completed_items.show()

product_performance = completed_items.groupBy("product_id", "product_name", "category") \
    .agg(
        spark_sum("quantity").alias("total_quantity_sold"),
        countDistinct("order_id").alias("orders_containing_product")
    ) \
    .orderBy(desc("total_quantity_sold"))

product_performance.write.mode("overwrite").saveAsTable("workspace.gold.product_performance")
display(spark.table("workspace.gold.product_performance"))

# COMMAND ----------

# DBTITLE 1,Category Performance
# Aggregate product performance up to category level
category_performance = completed_items.groupBy("category") \
    .agg(
        spark_sum("quantity").alias("total_quantity_sold"),
        countDistinct("order_id").alias("total_orders"),
        countDistinct("product_id").alias("unique_products_sold")
    ) \
    .orderBy(desc("total_quantity_sold"))

category_performance.write.mode("overwrite").saveAsTable("workspace.gold.category_performance")
display(spark.table("workspace.gold.category_performance"))

# COMMAND ----------

# DBTITLE 1,Monthly Order Trends
# Monthly breakdown of orders by status
monthly_trends = orders.withColumn("order_month", date_format(col("order_date"), "yyyy-MM")) \
    .groupBy("order_month") \
    .agg(
        count("*").alias("total_orders"),
        spark_sum(when(col("status") == "Completed", 1).otherwise(0)).alias("completed"),
        spark_sum(when(col("status") == "Cancelled", 1).otherwise(0)).alias("cancelled")
    ) \
    .orderBy("order_month")

monthly_trends.write.mode("overwrite").saveAsTable("workspace.gold.monthly_order_trends")
display(spark.table("workspace.gold.monthly_order_trends"))

# COMMAND ----------

# DBTITLE 1,Country Sales Summary
# Country-level summary: customers, orders, and total quantity from completed orders
country_joined = orders.join(order_items, "order_id", "left") \
    .join(customers, "customer_id") 

country_sales = country_joined.groupBy("country") \
    .agg(
        countDistinct("customer_id").alias("total_customers"),
        countDistinct("order_id").alias("total_orders"),
        spark_sum(when(col("status") == "Completed", col("quantity")).otherwise(0)).alias("total_quantity_sold")
    ) \
    .orderBy(desc("total_quantity_sold"))

country_sales.write.mode("overwrite").saveAsTable("workspace.gold.country_sales_summary")
display(spark.table("workspace.gold.country_sales_summary"))

# COMMAND ----------

# DBTITLE 1,Business KPIs
# Overall business KPIs — a single-row summary
completed_orders = orders.filter(col("status") == "Completed")
completed_order_ids = completed_orders.select("order_id")
completed_qty = order_items.join(completed_order_ids, "order_id").agg(spark_sum("quantity").alias("q")).collect()[0]["q"] or 0

kpis = spark.sql(f"""
SELECT
  (SELECT count(*) FROM workspace.silver.customers) AS total_customers,
  (SELECT count(*) FROM workspace.silver.products) AS total_products,
  (SELECT count(*) FROM workspace.silver.orders) AS total_orders,
  (SELECT count(*) FROM workspace.silver.orders WHERE status = 'Completed') AS completed_orders,
  (SELECT count(*) FROM workspace.silver.orders WHERE status = 'Cancelled') AS cancelled_orders,
  {completed_qty} AS total_quantity_sold,
  ROUND(
    (SELECT count(*) FROM workspace.silver.orders WHERE status = 'Completed') * 100.0 /
    (SELECT count(*) FROM workspace.silver.orders), 1
  ) AS order_completion_rate_pct
""")

kpis.write.mode("overwrite").saveAsTable("workspace.gold.business_kpis")
display(spark.table("workspace.gold.business_kpis"))