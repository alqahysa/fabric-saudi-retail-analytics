# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6297af8f-afdc-4f8c-9316-4c29cd7dd9c3",
# META       "default_lakehouse_name": "lh_retail",
# META       "default_lakehouse_workspace_id": "56093037-c36a-4035-b4e5-b42870864e55",
# META       "known_lakehouses": [
# META         {
# META           "id": "6297af8f-afdc-4f8c-9316-4c29cd7dd9c3"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from datetime import datetime, timezone
from pyspark.sql import functions as F

t = spark.table
results = []

def check(name, failing_rows):
    results.append((name, int(failing_rows), failing_rows == 0, datetime.now(timezone.utc)))

lines, orders = t("gold_fact_sales_lines"), t("gold_fact_orders")
CITIES = ["Riyadh", "Jeddah", "Makkah", "Madinah", "Dammam", "Khobar", "Taif",
          "Buraydah", "Abha", "Tabuk", "Hail", "Jazan", "Najran"]

# 1. Keys are unique and present
check("lines_pk_unique", lines.groupBy("order_id", "line_no").count().filter("count > 1").count())
check("orders_pk_unique", orders.groupBy("order_id").count().filter("count > 1").count())
check("lines_keys_not_null", lines.filter(F.col("product_key").isNull() | F.col("date_key").isNull()).count())

# 2. Every line finds its product, branch and date
check("lines_product_exists", lines.join(t("gold_dim_product"), "product_key", "left_anti").count())
check("lines_branch_exists", lines.join(t("gold_dim_branch"), "branch_key", "left_anti").count())
check("lines_date_exists", lines.join(t("gold_dim_date"), "date_key", "left_anti").count())

# 3. Standardized values: only the 13 known cities, no unparsed timestamps
check("city_is_standard", orders.filter(~F.col("city").isin(CITIES)).count())
check("order_hour_parsed", orders.filter(F.col("order_hour").isNull()).count())

# 4. Saudi VAT: 15% of net sales, allowing 1 halala of rounding per line
check("vat_is_15_percent",
      lines.filter(F.abs(F.col("vat_sar") - F.col("net_sales_sar") * 0.15) > 0.01).count())

# 5. Each payment equals its order's total including VAT (within 0.05 SAR)
check("payment_matches_order",
      orders.filter(F.abs(F.col("amount_paid_sar") - F.col("order_gross_sar")) > 0.05).count())

# 6. Net sales reconcile between silver and gold
silver_total = t("silver_order_items").agg(F.sum("net_sales_sar")).first()[0]
gold_total = lines.agg(F.sum("net_sales_sar")).first()[0]
check("net_sales_reconcile", 0 if silver_total == gold_total else 1)

# 7. Market data covers every category
check("market_covers_categories",
      t("gold_dim_category").join(t("gold_fact_market_monthly"), "category", "left_anti").count())

report = spark.createDataFrame(results, "check_name string, failing_rows long, passed boolean, checked_at timestamp")
report.write.format("delta").mode("append").saveAsTable("dq_results")
report.show(truncate=False)

failed = [r[0] for r in results if not r[2]]
if failed:
    raise Exception(f"Data quality checks failed: {failed}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
