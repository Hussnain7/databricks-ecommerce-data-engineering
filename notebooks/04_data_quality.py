# Databricks notebook source
# DBTITLE 1,Silver Layer Data Quality Checks
# MAGIC %md
# MAGIC # Silver Layer Data Quality Checks
# MAGIC
# MAGIC Comprehensive data quality validation for all 4 silver layer tables (`customers`, `products`, `orders`, `order_items`). Checks include null values, duplicate rows, duplicate IDs, negative values, invalid statuses, invalid dates, and referential integrity (foreign key validation).

# COMMAND ----------

# DBTITLE 1,data quality
from pyspark.sql.functions import col, current_date, lit

customers= spark.table("workspace.silver.customers")
products= spark.table("workspace.silver.products")
orders= spark.table("workspace.silver.orders")
order_items = spark.table("workspace.silver.order_items")


dq_results = []

null_checks = {
    "customers":   ["customer_id", "customer_name", "country"],
    "products":    ["product_id", "product_name", "category"],
    "orders":      ["order_id", "customer_id", "order_date", "status"],
    "order_items": ["order_item_id", "order_id", "product_id", "quantity"],
}

for tbl, cols in null_checks.items():
    df = spark.table(f"workspace.silver.{tbl}")
    for c in cols:
        n = df.filter(col(c).isNull()).count()
        status = "PASS" if n == 0 else "FAIL"
        dq_results.append((tbl, f"Null: {c}", n, status))
        marker = "✅" if n == 0 else "❌"
        print(f"{marker} {status} | {tbl}.{c}: {n} null(s)")

# DUPLICATE rows checks

_dup_tables = ["customers", "products", "orders", "order_items"]
_dup_cols = {tbl: spark.table(f"workspace.silver.{tbl}").columns for tbl in _dup_tables}

for tbl in _dup_tables:
    df = spark.table(f"workspace.silver.{tbl}")
    total = df.count()
    distinct = df.distinct().count()
    dup = total - distinct
    status = "PASS" if dup == 0 else "FAIL"
    dq_results.append((tbl, "Duplicate rows", dup, status))
    marker = "✅" if dup == 0 else "❌"
    print(f"{marker} {status} | {tbl}: {dup} duplicate row(s) out of {total}")
    if dup > 0:
        df.groupBy(_dup_cols[tbl]).count().filter("count > 1").show()


id_checks = {
    "customers":   "customer_id",
    "products":    "product_id",
    "orders":      "order_id",
    "order_items": "order_item_id",
}

for tbl, pk in id_checks.items():
    df = spark.table(f"workspace.silver.{tbl}")
    dups = df.groupBy(pk).count().filter("count > 1")
    n = dups.count()
    status = "PASS" if n == 0 else "FAIL"
    dq_results.append((tbl, f"Duplicate {pk}", n, status))
    marker = "✅" if n == 0 else "❌"
    print(f"{marker} {status} | {tbl}.{pk}: {n} duplicate ID(s)")
    if n > 0:
        dups.show()

# ─────────────────────────────────────────────
# 4. NEGATIVE VALUES CHECK (price & quantity)
# ─────────────────────────────────────────────


# Negative price – products table has no price column
dq_results.append(("products", "Negative price (no price col)", 0, "SKIPPED"))
print("⏭️  SKIPPED | products: no 'price' column exists in schema")

# Negative quantity in order_items
neg_qty = spark.table("workspace.silver.order_items").filter(col("quantity") < 0)
n = neg_qty.count()
status = "PASS" if n == 0 else "FAIL"
dq_results.append(("order_items", "Negative quantity", n, status))
marker = "✅" if n == 0 else "❌"
print(f"{marker} {status} | order_items.quantity: {n} row(s) with negative quantity")
if n > 0:
    neg_qty.show()

# ─────────────────────────────────────────────
# 5. INVALID STATUS CHECK (orders table)
# ─────────────────────────────────────────────

valid_statuses = ["Completed", "Cancelled"]
bad_status = spark.table("workspace.silver.orders").filter(
    col("status").isNull() | ~col("status").isin(valid_statuses)
)
n = bad_status.count()
status = "PASS" if n == 0 else "FAIL"
dq_results.append(("orders", "Invalid status", n, status))
marker = "✅" if n == 0 else "❌"
print(f"{marker} {status} | orders.status: {n} row(s) with invalid status")
print(f"   Valid statuses: {valid_statuses}")
if n > 0:
    bad_status.show()


# ─────────────────────────────────────────────
# 6. INVALID DATES CHECK (orders table)
#    Flags: null dates, future dates, dates before 2000-01-01
# ─────────────────────────────────────────────

bad_dates = spark.table("workspace.silver.orders").filter(
    col("order_date").isNull()
    | (col("order_date") > current_date())
    | (col("order_date") < lit("2000-01-01"))
)
n = bad_dates.count()
status = "PASS" if n == 0 else "FAIL"
dq_results.append(("orders", "Invalid date", n, status))
marker = "✅" if n == 0 else "❌"
print(f"{marker} {status} | orders.order_date: {n} row(s) with invalid date")
print("   (null, future, or pre-2000)")
if n > 0:
    bad_dates.show()

# ─────────────────────────────────────────────
# 7. REFERENTIAL INTEGRITY CHECKS (foreign keys)
#    – orders.customer_id      → customers.customer_id
#    – order_items.product_id  → products.product_id
#    – order_items.order_id     → orders.order_id
# ─────────────────────────────────────────────

# Invalid customer IDs in orders
bad_cust = spark.table("workspace.silver.orders").join(
    spark.table("workspace.silver.customers"), on="customer_id", how="left_anti"
)
n = bad_cust.count()
status = "PASS" if n == 0 else "FAIL"
dq_results.append(("orders", "Invalid customer_id (FK)", n, status))
marker = "✅" if n == 0 else "❌"
print(f"{marker} {status} | orders.customer_id: {n} order(s) with invalid customer_id")
if n > 0:
    bad_cust.show()

# Invalid product IDs in order_items
bad_prod = spark.table("workspace.silver.order_items").join(
    spark.table("workspace.silver.products"), on="product_id", how="left_anti"
)
n = bad_prod.count()
status = "PASS" if n == 0 else "FAIL"
dq_results.append(("order_items", "Invalid product_id (FK)", n, status))
marker = "✅" if n == 0 else "❌"
print(f"{marker} {status} | order_items.product_id: {n} row(s) with invalid product_id")
if n > 0:
    bad_prod.show()

# Invalid order IDs in order_items
bad_order = spark.table("workspace.silver.order_items").join(
    spark.table("workspace.silver.orders"), on="order_id", how="left_anti"
)
n = bad_order.count()
status = "PASS" if n == 0 else "FAIL"
dq_results.append(("order_items", "Invalid order_id (FK)", n, status))
marker = "✅" if n == 0 else "❌"
print(f"{marker} {status} | order_items.order_id: {n} row(s) with invalid order_id")
if n > 0:
    bad_order.show()

# ─────────────────────────────────────────────
# DATA QUALITY SUMMARY
# ─────────────────────────────────────────────

summary_df = spark.createDataFrame(dq_results, ["table", "check", "violation_count", "status"])
display(summary_df)

total   = len(dq_results)
passed  = sum(1 for r in dq_results if r[3] == "PASS")
failed  = sum(1 for r in dq_results if r[3] == "FAIL")
skipped = sum(1 for r in dq_results if r[3] == "SKIPPED")

print(f"\nTotal checks: {total}  |  Passed: {passed}  |  Failed: {failed}  |  Skipped: {skipped}")
if failed == 0 and skipped == total:
    print("⚠️  All checks were skipped — review table schemas.")
elif failed == 0:
    print("🎉 All data quality checks passed!")
else:
    print(f"⚠️  {failed} check(s) failed — review violations above.")














# COMMAND ----------

# DBTITLE 1,Null Values Check
from pyspark.sql.functions import col, current_date, lit

# Initialize DQ results accumulator – each check appends a tuple
# (table, check_name, violation_count, status)
dq_results = []

# ─────────────────────────────────────────────
# 1. NULL VALUES CHECK (all 4 tables, all columns)
# ─────────────────────────────────────────────


null_checks = {
    "customers":   ["customer_id", "customer_name", "country"],
    "products":    ["product_id", "product_name", "category"],
    "orders":      ["order_id", "customer_id", "order_date", "status"],
    "order_items": ["order_item_id", "order_id", "product_id", "quantity"],
}

for tbl, cols in null_checks.items():
    df = spark.table(f"workspace.silver.{tbl}")
    for c in cols:
        n = df.filter(col(c).isNull()).count()
        status = "PASS" if n == 0 else "FAIL"
        dq_results.append((tbl, f"Null: {c}", n, status))
        marker = "✅" if n == 0 else "❌"
        print(f"{marker} {status} | {tbl}.{c}: {n} null(s)")


# COMMAND ----------

# DBTITLE 1,Duplicate Rows Check
table_names = ["customers", "products", "orders", "order_items"]
table_cols = {tbl: spark.table(f"workspace.silver.{tbl}").columns for tbl in table_names}

for tbl in table_names:
    df = spark.table(f"workspace.silver.{tbl}")
    cols = table_cols[tbl]
    total = df.count()
    dups = df.groupBy(cols).count().filter("count > 1")
    dup = dups.count()
    status = "PASS" if dup == 0 else "FAIL"
    dq_results.append((tbl, "Duplicate rows", dup, status))
    marker = "✅" if dup == 0 else "❌"
    print(f"{marker} {status} | {tbl}: {dup} duplicate row(s) out of {total}")
    if dup > 0:
        dups.show()

# COMMAND ----------

# DBTITLE 1,Duplicate IDs Check
id_checks = {
    "customers":   "customer_id",
    "products":    "product_id",
    "orders":      "order_id",
    "order_items": "order_item_id",
}

for tbl, pk in id_checks.items():
    df = spark.table(f"workspace.silver.{tbl}")
    dups = df.groupBy(pk).count().filter("count > 1")
    n = dups.count()
    status = "PASS" if n == 0 else "FAIL"
    dq_results.append((tbl, f"Duplicate {pk}", n, status))
    marker = "✅" if n == 0 else "❌"
    print(f"{marker} {status} | {tbl}.{pk}: {n} duplicate ID(s)")
    if n > 0:
        dups.show()


# COMMAND ----------

# DBTITLE 1,Negative Values Check
# ─────────────────────────────────────────────
# 4. NEGATIVE VALUES CHECK (price & quantity)
# ─────────────────────────────────────────────


# Negative price – products table has no price column
dq_results.append(("products", "Negative price (no price col)", 0, "SKIPPED"))
print("⏭️  SKIPPED | products: no 'price' column exists in schema")

# Negative quantity in order_items
neg_qty = spark.table("workspace.silver.order_items").filter(col("quantity") < 0)
n = neg_qty.count()
status = "PASS" if n == 0 else "FAIL"
dq_results.append(("order_items", "Negative quantity", n, status))
marker = "✅" if n == 0 else "❌"
print(f"{marker} {status} | order_items.quantity: {n} row(s) with negative quantity")
if n > 0:
    neg_qty.show()


# COMMAND ----------

# DBTITLE 1,Invalid Status Check
# ─────────────────────────────────────────────
# 5. INVALID STATUS CHECK (orders table)
# ─────────────────────────────────────────────

valid_statuses = ["Completed", "Cancelled"]
bad_status = spark.table("workspace.silver.orders").filter(
    col("status").isNull() | ~col("status").isin(valid_statuses)
)
n = bad_status.count()
status = "PASS" if n == 0 else "FAIL"
dq_results.append(("orders", "Invalid status", n, status))
marker = "✅" if n == 0 else "❌"
print(f"{marker} {status} | orders.status: {n} row(s) with invalid status")
print(f"   Valid statuses: {valid_statuses}")
if n > 0:
    bad_status.show()


# COMMAND ----------

# DBTITLE 1,Invalid Dates Check
# ─────────────────────────────────────────────
# 6. INVALID DATES CHECK (orders table)
#    Flags: null dates, future dates, dates before 2000-01-01
# ─────────────────────────────────────────────

bad_dates = spark.table("workspace.silver.orders").filter(
    col("order_date").isNull()
    | (col("order_date") > current_date())
    | (col("order_date") < lit("2000-01-01"))
)
n = bad_dates.count()
status = "PASS" if n == 0 else "FAIL"
dq_results.append(("orders", "Invalid date", n, status))
marker = "✅" if n == 0 else "❌"
print(f"{marker} {status} | orders.order_date: {n} row(s) with invalid date")
print("   (null, future, or pre-2000)")
if n > 0:
    bad_dates.show()


# COMMAND ----------

# DBTITLE 1,Referential Integrity Checks
# ─────────────────────────────────────────────
# 7. REFERENTIAL INTEGRITY CHECKS (foreign keys)
#    – orders.customer_id      → customers.customer_id
#    – order_items.product_id  → products.product_id
#    – order_items.order_id     → orders.order_id
# ─────────────────────────────────────────────

# Invalid customer IDs in orders
bad_cust = spark.table("workspace.silver.orders").join(
    spark.table("workspace.silver.customers"), on="customer_id", how="left_anti"
)
n = bad_cust.count()
status = "PASS" if n == 0 else "FAIL"
dq_results.append(("orders", "Invalid customer_id (FK)", n, status))
marker = "✅" if n == 0 else "❌"
print(f"{marker} {status} | orders.customer_id: {n} order(s) with invalid customer_id")
if n > 0:
    bad_cust.show()

# Invalid product IDs in order_items
bad_prod = spark.table("workspace.silver.order_items").join(
    spark.table("workspace.silver.products"), on="product_id", how="left_anti"
)
n = bad_prod.count()
status = "PASS" if n == 0 else "FAIL"
dq_results.append(("order_items", "Invalid product_id (FK)", n, status))
marker = "✅" if n == 0 else "❌"
print(f"{marker} {status} | order_items.product_id: {n} row(s) with invalid product_id")
if n > 0:
    bad_prod.show()

# Invalid order IDs in order_items
bad_order = spark.table("workspace.silver.order_items").join(
    spark.table("workspace.silver.orders"), on="order_id", how="left_anti"
)
n = bad_order.count()
status = "PASS" if n == 0 else "FAIL"
dq_results.append(("order_items", "Invalid order_id (FK)", n, status))
marker = "✅" if n == 0 else "❌"
print(f"{marker} {status} | order_items.order_id: {n} row(s) with invalid order_id")
if n > 0:
    bad_order.show()


# COMMAND ----------

# DBTITLE 1,Data Quality Summary
# ─────────────────────────────────────────────
# DATA QUALITY SUMMARY
# ─────────────────────────────────────────────

summary_df = spark.createDataFrame(dq_results, ["table", "check", "violation_count", "status"])
display(summary_df)

total   = len(dq_results)
passed  = sum(1 for r in dq_results if r[3] == "PASS")
failed  = sum(1 for r in dq_results if r[3] == "FAIL")
skipped = sum(1 for r in dq_results if r[3] == "SKIPPED")

print(f"\nTotal checks: {total}  |  Passed: {passed}  |  Failed: {failed}  |  Skipped: {skipped}")
if failed == 0 and skipped == total:
    print("⚠️  All checks were skipped — review table schemas.")
elif failed == 0:
    print("🎉 All data quality checks passed!")
else:
    print(f"⚠️  {failed} check(s) failed — review violations above.")
