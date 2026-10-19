# Databricks notebook source
# DBTITLE 1,gold_data_quality
from pyspark.sql.functions import col, lit, count, sum as spark_sum, when, countDistinct, date_format

c_o_summary=spark.table("workspace.gold.customer_order_summary")
p_performance=spark.table("workspace.gold.product_performance")
c_performance=spark.table("workspace.gold.category_performance")
m_o_trends=spark.table("workspace.gold.monthly_order_trends")
sales_summary=spark.table("workspace.gold.country_sales_summary")
business_kpis=spark.table("workspace.gold.business_kpis")

# Silver tables for gold vs silver reconciliation
orders=spark.table("workspace.silver.orders")
order_items=spark.table("workspace.silver.order_items")
customers=spark.table("workspace.silver.customers")
products=spark.table("workspace.silver.products")

#NULL CHECK

result = []

nullcheck = {
    "customer_order_summary": ["customer_id", "customer_name", "country", "total_orders", "completed_orders", "cancelled_orders", "total_quantity_ordered"],
    "product_performance": ["product_id", "product_name", "category", "total_quantity_sold", "orders_containing_product"],
    "category_performance": ["category", "total_quantity_sold", "total_orders", "unique_products_sold"],
    "monthly_order_trends": ["order_month", "total_orders", "completed", "cancelled"],
    "country_sales_summary": ["country", "total_customers", "total_orders", "total_quantity_sold"],
    "business_kpis": ["total_customers", "total_products", "total_orders", "completed_orders", "cancelled_orders", "total_quantity_sold", "order_completion_rate_pct"]}



for tbl,cols in nullcheck.items():
    df =spark.table(f"workspace.gold.{tbl}")
    for c in cols:
        n = df.filter(col(c).isNull()).count()
        status= "Pass" if n==0 else "fail"
        result.append(( tbl, f"column: {c}", n, status))
        print(f" {status} | Table: {tbl}.{c} :{n} null(s)")
                                          
#Duplicate rows check

tables = ["customer_order_summary", "product_performance", "category_performance", "monthly_order_trends", "country_sales_summary", "business_kpis"]

for tbl in tables:
    df =spark.table(f"workspace.gold.{tbl}")
    n = df.count()
    dup = df.dropDuplicates().count()
    status= "Pass" if n==dup else "fail"
    result.append(( tbl, f"Duplicate rows", n-dup, status))
    print(f" {status} | Table: {tbl} :{n-dup} duplicate rows")

#Duplicate IDs check

dup_id = {
    "customer_order_summary": ["customer_id"],
    "product_performance": ["product_id"],
    "category_performance": ["category"],
    "monthly_order_trends": ["order_month"],
    "country_sales_summary": ["country"]
}

for tbl,cols in dup_id.items():
    df =spark.table(f"workspace.gold.{tbl}")
    for c in cols:
        dup=df.groupBy(c).count().filter(col("count")>1)
        null=df.filter(col(c).isNull()).count()
        n = dup.count()
        status= "Pass" if n==0 and null==0 else "Fail"
        result.append(( tbl, f"Duplicate IDs", n, status))
        print(f" {status} | Table: {tbl}.{c} :{n} duplicate ID(s)")

#Negative/impossible values

neg_check = {
    "customer_order_summary": ["total_orders", "completed_orders", "cancelled_orders", "total_quantity_ordered"],
    "product_performance": ["total_quantity_sold", "orders_containing_product"],
    "category_performance": ["total_quantity_sold", "total_orders", "unique_products_sold"],
    "monthly_order_trends": ["total_orders", "completed", "cancelled"],
    "country_sales_summary": ["total_customers", "total_orders", "total_quantity_sold"],
    "business_kpis": ["total_customers", "total_products", "total_orders", "completed_orders", "cancelled_orders", "total_quantity_sold"]
}

for tbl, cols in neg_check.items():
    df = spark.table(f"workspace.gold.{tbl}")
    for c in cols:
        n = df.filter(col(c) < 0).count()
        status = "Pass" if n == 0 else "Fail"
        result.append((tbl, f"Negative values: {c}", n, status))
        print(f" {status} | Table: {tbl}.{c} :{n} negative value(s)")

# order_completion_rate_pct must be between 0 and 100
bk = spark.table("workspace.gold.business_kpis")
n = bk.filter((col("order_completion_rate_pct") < 0) | (col("order_completion_rate_pct") > 100)).count()
status = "Pass" if n == 0 else "Fail"
result.append(("business_kpis", "order_completion_rate_pct out of range [0-100]", n, status))
print(f" {status} | Table: business_kpis.order_completion_rate_pct :{n} out-of-range value(s)")

# completed_orders + cancelled_orders should not exceed total_orders
for tbl, comp_col, canc_col in [
    ("customer_order_summary", "completed_orders", "cancelled_orders"),
    ("monthly_order_trends", "completed", "cancelled"),
    ("business_kpis", "completed_orders", "cancelled_orders"),
]:
    df = spark.table(f"workspace.gold.{tbl}")
    n = df.filter((col(comp_col) + col(canc_col)) > col("total_orders")).count()
    status = "Pass" if n == 0 else "Fail"
    result.append((tbl, f"completed+cancelled > total_orders", n, status))
    print(f" {status} | Table: {tbl} :{n} row(s) where completed+cancelled exceeds total_orders")

#Business Rule Validation

# Rule 1: completed_orders and cancelled_orders must not exceed total_orders
for tbl, comp_col, canc_col in [
    ("customer_order_summary", "completed_orders", "cancelled_orders"),
    ("business_kpis", "completed_orders", "cancelled_orders"),
]:
    df = spark.table(f"workspace.gold.{tbl}")
    for check_col in [comp_col, canc_col]:
        n = df.filter(col(check_col) > col("total_orders")).count()
        status = "Pass" if n == 0 else "Fail"
        result.append((tbl, f"Business rule: {check_col} <= total_orders", n, status))
        print(f" {status} | Table: {tbl} :{n} row(s) where {check_col} > total_orders")

# Rule 2: order_completion_rate_pct must equal round(completed_orders * 100.0 / total_orders, 1)
bk_row = spark.table("workspace.gold.business_kpis").collect()[0]
expected_rate = round(bk_row["completed_orders"] * 100.0 / bk_row["total_orders"], 1)
actual_rate = float(bk_row["order_completion_rate_pct"])
n = 0 if abs(actual_rate - expected_rate) < 0.01 else 1
status = "Pass" if n == 0 else "Fail"
result.append(("business_kpis", "Business rule: completion rate formula", n, status))
print(f" {status} | business_kpis: expected {expected_rate}, actual {actual_rate}")

# Rule 3: Cross-table consistency — sums from detail gold tables must match business_kpis
cos = spark.table("workspace.gold.customer_order_summary")
mot = spark.table("workspace.gold.monthly_order_trends")
pp = spark.table("workspace.gold.product_performance")
cat = spark.table("workspace.gold.category_performance")

cross_checks = [
    ("customer_order_summary.total_orders", cos.agg(spark_sum("total_orders")).collect()[0][0], bk_row["total_orders"]),
    ("customer_order_summary.completed_orders", cos.agg(spark_sum("completed_orders")).collect()[0][0], bk_row["completed_orders"]),
    ("customer_order_summary.cancelled_orders", cos.agg(spark_sum("cancelled_orders")).collect()[0][0], bk_row["cancelled_orders"]),
    ("monthly_order_trends.total_orders", mot.agg(spark_sum("total_orders")).collect()[0][0], bk_row["total_orders"]),
    ("monthly_order_trends.completed", mot.agg(spark_sum("completed")).collect()[0][0], bk_row["completed_orders"]),
    ("monthly_order_trends.cancelled", mot.agg(spark_sum("cancelled")).collect()[0][0], bk_row["cancelled_orders"]),
    ("product_performance.total_quantity_sold", pp.agg(spark_sum("total_quantity_sold")).collect()[0][0], bk_row["total_quantity_sold"]),
    ("category_performance.total_quantity_sold", cat.agg(spark_sum("total_quantity_sold")).collect()[0][0], bk_row["total_quantity_sold"]),
]

for label, detail_val, kpi_val in cross_checks:
    n = 0 if detail_val == kpi_val else 1
    status = "Pass" if n == 0 else "Fail"
    result.append(("cross_table", f"Business rule: {label} == business_kpis", n, status))
    print(f" {status} | Cross-check: {label} = {detail_val} vs KPI = {kpi_val}")

#Gold vs Silver Reconciliation

# 1. customer_order_summary
silver_cos = orders.join(order_items, "order_id", "left").join(customers, "customer_id", "left").groupBy("customer_id", "customer_name", "country").agg(
    countDistinct("order_id").alias("total_orders"),
    countDistinct(when(col("status") == "Completed", col("order_id")).otherwise(None)).alias("completed_orders"),
    countDistinct(when(col("status") == "Cancelled", col("order_id")).otherwise(None)).alias("cancelled_orders"),
    spark_sum("quantity").alias("total_quantity_ordered"))
gold_cos = spark.table("workspace.gold.customer_order_summary")
n = 0 if silver_cos.count() == gold_cos.count() else 1
status = "Pass" if n == 0 else "Fail"
result.append(("customer_order_summary", "Gold vs Silver: row count", n, status))
print(f" {status} | customer_order_summary: gold={gold_cos.count()} vs silver={silver_cos.count()} rows")
for metric in ["total_orders", "completed_orders", "cancelled_orders", "total_quantity_ordered"]:
    g = gold_cos.agg(spark_sum(metric)).collect()[0][0]
    s = silver_cos.agg(spark_sum(metric)).collect()[0][0]
    n = 0 if g == s else 1
    status = "Pass" if n == 0 else "Fail"
    result.append(("customer_order_summary", f"Gold vs Silver: {metric}", n, status))
    print(f" {status} | customer_order_summary.{metric}: gold={g} vs silver={s}")

# 2. product_performance
silver_pp = order_items.join(orders, "order_id").filter(col("status") == "Completed").join(products, "product_id").groupBy("product_id", "product_name", "category").agg(
    spark_sum("quantity").alias("total_quantity_sold"),
    countDistinct("order_id").alias("orders_containing_product"))
gold_pp = spark.table("workspace.gold.product_performance")
n = 0 if silver_pp.count() == gold_pp.count() else 1
status = "Pass" if n == 0 else "Fail"
result.append(("product_performance", "Gold vs Silver: row count", n, status))
print(f" {status} | product_performance: gold={gold_pp.count()} vs silver={silver_pp.count()} rows")
for metric in ["total_quantity_sold", "orders_containing_product"]:
    g = gold_pp.agg(spark_sum(metric)).collect()[0][0]
    s = silver_pp.agg(spark_sum(metric)).collect()[0][0]
    n = 0 if g == s else 1
    status = "Pass" if n == 0 else "Fail"
    result.append(("product_performance", f"Gold vs Silver: {metric}", n, status))
    print(f" {status} | product_performance.{metric}: gold={g} vs silver={s}")

# 3. category_performance
silver_cat = order_items.join(orders, "order_id").filter(col("status") == "Completed").join(products, "product_id").groupBy("category").agg(
    spark_sum("quantity").alias("total_quantity_sold"),
    countDistinct("order_id").alias("total_orders"),
    countDistinct("product_id").alias("unique_products_sold"))
gold_cat = spark.table("workspace.gold.category_performance")
n = 0 if silver_cat.count() == gold_cat.count() else 1
status = "Pass" if n == 0 else "Fail"
result.append(("category_performance", "Gold vs Silver: row count", n, status))
print(f" {status} | category_performance: gold={gold_cat.count()} vs silver={silver_cat.count()} rows")
for metric in ["total_quantity_sold", "total_orders", "unique_products_sold"]:
    g = gold_cat.agg(spark_sum(metric)).collect()[0][0]
    s = silver_cat.agg(spark_sum(metric)).collect()[0][0]
    n = 0 if g == s else 1
    status = "Pass" if n == 0 else "Fail"
    result.append(("category_performance", f"Gold vs Silver: {metric}", n, status))
    print(f" {status} | category_performance.{metric}: gold={g} vs silver={s}")

# 4. monthly_order_trends
silver_mot = orders.withColumn("order_month", date_format(col("order_date"), "yyyy-MM")).groupBy("order_month").agg(
    count("*").alias("total_orders"),
    spark_sum(when(col("status") == "Completed", 1).otherwise(0)).alias("completed"),
    spark_sum(when(col("status") == "Cancelled", 1).otherwise(0)).alias("cancelled"))
gold_mot = spark.table("workspace.gold.monthly_order_trends")
n = 0 if silver_mot.count() == gold_mot.count() else 1
status = "Pass" if n == 0 else "Fail"
result.append(("monthly_order_trends", "Gold vs Silver: row count", n, status))
print(f" {status} | monthly_order_trends: gold={gold_mot.count()} vs silver={silver_mot.count()} rows")
for metric in ["total_orders", "completed", "cancelled"]:
    g = gold_mot.agg(spark_sum(metric)).collect()[0][0]
    s = silver_mot.agg(spark_sum(metric)).collect()[0][0]
    n = 0 if g == s else 1
    status = "Pass" if n == 0 else "Fail"
    result.append(("monthly_order_trends", f"Gold vs Silver: {metric}", n, status))
    print(f" {status} | monthly_order_trends.{metric}: gold={g} vs silver={s}")

# 5. country_sales_summary
silver_css = orders.join(order_items, "order_id", "left").join(customers, "customer_id").groupBy("country").agg(
    countDistinct("customer_id").alias("total_customers"),
    countDistinct("order_id").alias("total_orders"),
    spark_sum(when(col("status") == "Completed", col("quantity")).otherwise(0)).alias("total_quantity_sold"))
gold_css = spark.table("workspace.gold.country_sales_summary")
n = 0 if silver_css.count() == gold_css.count() else 1
status = "Pass" if n == 0 else "Fail"
result.append(("country_sales_summary", "Gold vs Silver: row count", n, status))
print(f" {status} | country_sales_summary: gold={gold_css.count()} vs silver={silver_css.count()} rows")
for metric in ["total_customers", "total_orders", "total_quantity_sold"]:
    g = gold_css.agg(spark_sum(metric)).collect()[0][0]
    s = silver_css.agg(spark_sum(metric)).collect()[0][0]
    n = 0 if g == s else 1
    status = "Pass" if n == 0 else "Fail"
    result.append(("country_sales_summary", f"Gold vs Silver: {metric}", n, status))
    print(f" {status} | country_sales_summary.{metric}: gold={g} vs silver={s}")

# 6. business_kpis
silver_kpis = spark.sql("""
SELECT
  (SELECT count(*) FROM workspace.silver.customers) AS total_customers,
  (SELECT count(*) FROM workspace.silver.products) AS total_products,
  (SELECT count(*) FROM workspace.silver.orders) AS total_orders,
  (SELECT count(*) FROM workspace.silver.orders WHERE status = 'Completed') AS completed_orders,
  (SELECT count(*) FROM workspace.silver.orders WHERE status = 'Cancelled') AS cancelled_orders,
  (SELECT COALESCE(sum(oi.quantity), 0) FROM workspace.silver.order_items oi JOIN workspace.silver.orders o ON oi.order_id = o.order_id WHERE o.status = 'Completed') AS total_quantity_sold,
  ROUND((SELECT count(*) FROM workspace.silver.orders WHERE status = 'Completed') * 100.0 / (SELECT count(*) FROM workspace.silver.orders), 1) AS order_completion_rate_pct
""")
gold_kpis = spark.table("workspace.gold.business_kpis")
for metric in ["total_customers", "total_products", "total_orders", "completed_orders", "cancelled_orders", "total_quantity_sold", "order_completion_rate_pct"]:
    g = gold_kpis.select(metric).collect()[0][0]
    s = silver_kpis.select(metric).collect()[0][0]
    n = 0 if str(g) == str(s) else 1
    status = "Pass" if n == 0 else "Fail"
    result.append(("business_kpis", f"Gold vs Silver: {metric}", n, status))
    print(f" {status} | business_kpis.{metric}: gold={g} vs silver={s}")

#Summary
print("\n" + "="*80)
print(f"DATA QUALITY SUMMARY: {len(result)} total checks")
fail_count = sum(1 for r in result if r[3] == "Fail")
print(f"Pass: {len(result) - fail_count} | Fail: {fail_count}")

