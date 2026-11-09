# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "fa6566e2-60d8-431a-86aa-c274ea8f752b",
# META       "default_lakehouse_name": "lh_retail",
# META       "default_lakehouse_workspace_id": "2d67245a-291e-4232-acfa-c6032bc23a2d",
# META       "known_lakehouses": [
# META         {
# META           "id": "fa6566e2-60d8-431a-86aa-c274ea8f752b"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # nb_03_gold_model
# Gold layer: star schema and market benchmark, rebuilt in full from silver on every run (small, under a minute).

# CELL ********************

from datetime import date
from pyspark.sql import functions as F

def save(df, name):
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").saveAsTable(name))
    print(f"{name}: {spark.table(name).count():,} rows")

t = spark.table
orders, lines, pays = t("silver_orders"), t("silver_order_items"), t("silver_payments")
events = t("silver_events")
date_key = lambda col: F.date_format(col, "yyyyMMdd").cast("int")

# dim_date: Saudi weekend, salary days, events
lo, hi = orders.agg(F.min("order_date"), F.max("order_date")).first()
print(f"Sales from {lo} to {hi}; calendar {lo.year}-01-01 to {hi.year}-12-31")
cal = spark.sql(f"SELECT explode(sequence(DATE'{date(lo.year, 1, 1)}', DATE'{date(hi.year, 12, 31)}', "
                f"INTERVAL 1 DAY)) AS date")
event_day = (cal.join(events, (cal.date >= events.start_date) & (cal.date <= events.end_date))
                .groupBy("date").agg(F.first("event_name").alias("event_name")))

# last payday: the 27th of this month, or of the previous month before the 27th
last_payday = (F.when(F.dayofmonth("date") >= 27, F.date_add(F.trunc("date", "month"), 26))
                .otherwise(F.date_add(F.trunc(F.add_months("date", -1), "month"), 26)))

dim_date = (cal.join(event_day, "date", "left")
    .select(date_key("date").alias("date_key"), "date",
            F.year("date").alias("year"), F.quarter("date").alias("quarter"),
            F.month("date").alias("month"), F.date_format("date", "MMM").alias("month_name"),
            F.date_format("date", "yyyy-MM").alias("year_month"),
            F.date_format("date", "EEEE").alias("day_name"),
            F.dayofweek("date").isin(6, 7).alias("is_weekend"),          # Friday, Saturday
            ((F.dayofmonth("date") >= 27) | (F.dayofmonth("date") <= 2)).alias("is_salary_period"),
            F.datediff("date", last_payday).alias("days_since_salary"),
            F.coalesce("event_name", F.lit("Regular day")).alias("event_name")))
save(dim_date, "gold_dim_date")

# dimensions
history = (orders.groupBy("customer_id")
    .agg(F.min("order_date").alias("first_order_date"),
         F.countDistinct("order_id").alias("lifetime_orders")))
save(t("silver_customers").join(history, "customer_id", "left")
       .withColumnRenamed("customer_id", "customer_key")
       .withColumn("is_repeat_customer", F.col("lifetime_orders") > 1), "gold_dim_customer")
save(t("silver_products").withColumnRenamed("product_id", "product_key"), "gold_dim_product")
save(t("silver_products").select("category", "sama_sector").distinct(), "gold_dim_category")
save(t("silver_branches").withColumnRenamed("branch_id", "branch_key")
       .withColumn("is_online", F.col("branch_key") == "WEB"), "gold_dim_branch")

# fact_sales_lines
save(lines.join(orders.select("order_id", "customer_id", "branch_id", "channel", "city",
                              "order_date", "status"), "order_id")
     .select("order_id", "line_no", date_key("order_date").alias("date_key"),
             F.col("customer_id").alias("customer_key"), F.col("product_id").alias("product_key"),
             F.col("branch_id").alias("branch_key"), "channel", "city", "status", "quantity",
             "unit_price_sar", "discount_sar", "net_sales_sar", "vat_sar", "gross_sales_sar"),
     "gold_fact_sales_lines")

# fact_orders
basket = lines.groupBy("order_id").agg(F.sum("quantity").alias("items"),
                                      F.sum("gross_sales_sar").alias("order_gross_sar"))
save(orders.join(pays.filter("payment_seq = 1"), "order_id", "left")
           .join(basket, "order_id", "left")
     .select("order_id", date_key("order_date").alias("date_key"),
             F.col("customer_id").alias("customer_key"), F.col("branch_id").alias("branch_key"),
             "channel", "city", "status", F.hour("ordered_at").alias("order_hour"),
             "payment_method", "installments", "is_bnpl",
             F.col("amount_sar").alias("amount_paid_sar"), "items", "order_gross_sar"),
     "gold_fact_orders")

# fact_market_monthly: SAMA sector spending mapped to our categories
market = (t("silver_market_monthly")
    .join(t("gold_dim_category"), F.col("sector") == F.col("sama_sector"))
    # keep only months the calendar covers, so every date_key exists in gold_dim_date
    .filter(F.col("month").between(F.lit(str(date(lo.year, 1, 1))), F.lit(str(date(hi.year, 12, 31)))))
    .select(date_key("month").alias("date_key"), "category", "month", "market_sales_sar"))
save(market, "gold_fact_market_monthly")

# mart_benchmark_monthly: both series indexed to 100 in the first month
ours = (lines.join(orders.filter("status = 'delivered'").select("order_id", "order_date"), "order_id")
             .join(t("silver_products").select("product_id", "category"), "product_id")
             .groupBy(F.trunc("order_date", "month").alias("month"), "category")
             .agg(F.sum("net_sales_sar").cast("double").alias("our_net_sales_sar")))
bench = ours.join(market.select("month", "category", "market_sales_sar"), ["month", "category"])
first = (bench.groupBy("category").agg(F.min("month").alias("month"))
              .join(bench, ["category", "month"])
              .select("category", F.col("our_net_sales_sar").alias("our_base"),
                      F.col("market_sales_sar").alias("market_base")))
save(bench.join(first, "category")
     .select("month", date_key("month").alias("date_key"), "category",
             "our_net_sales_sar", "market_sales_sar",
             (F.col("our_net_sales_sar") / F.col("our_base") * 100).alias("our_index"),
             (F.col("market_sales_sar") / F.col("market_base") * 100).alias("market_index")),
     "gold_mart_benchmark_monthly")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
