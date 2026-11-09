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

# # nb_02_silver_transform
# Silver layer: clean bronze into typed tables. Sales tables load **incrementally** (watermark + MERGE, Step 5);
# small reference tables are rebuilt in full. Safe to run from a pipeline every month.

# MARKDOWN ********************

# ## Parameters (toggle this cell as the parameter cell)
# `RESET_SILVER = True` only to rebuild the silver sales tables from scratch, once, by hand. A pipeline never sets it.

# PARAMETERS CELL ********************

RESET_SILVER = False

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Helpers

# CELL ********************

from pyspark.sql import functions as F, Window as W

# Money as decimals with VAT kept separate from net sales
def money(col):
    return F.col(col).cast("decimal(12,2)")

# Removes duplicates: for each key it keeps one row, the most recently loaded
def keep_latest(df, keys):
    w = W.partitionBy(*keys).orderBy(F.col("_ingested_at").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

# One timestamp type despite the format change
def parse_ts(col):
    """ISO timestamps normally; day-first after the July 2024 system change."""
    c = F.trim(F.col(col))
    return (F.when(c.rlike(r"^\d{4}-"), F.to_timestamp(c, "yyyy-MM-dd HH:mm:ss"))
             .otherwise(F.to_timestamp(c, "dd/MM/yyyy HH:mm")))

# Every spelling seen in the source -> one English name (keys in lower case)
CITY_MAP = {
    "الرياض": "Riyadh", "riyadh": "Riyadh", "riyadh city": "Riyadh",
    "جدة": "Jeddah", "jeddah": "Jeddah", "jiddah": "Jeddah",
    "مكة المكرمة": "Makkah", "makkah": "Makkah", "mecca": "Makkah",
    "المدينة المنورة": "Madinah", "madinah": "Madinah", "medina": "Madinah",
    "الدمام": "Dammam", "dammam": "Dammam",
}
city_lookup = F.create_map([F.lit(x) for pair in CITY_MAP.items() for x in pair])


def standard_city(col):
    raw = F.trim(F.col(col))
    return F.coalesce(city_lookup[F.lower(raw)], F.initcap(raw))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Cleaning functions (one per table)

# CELL ********************

def clean_orders(df):
    out = df.select(
        F.trim("order_id").alias("order_id"),
        F.trim("customer_id").alias("customer_id"),
        F.trim("branch_id").alias("branch_id"),
        F.trim("channel").alias("channel"),
        standard_city("city").alias("city"),
        parse_ts("order_ts").alias("ordered_at"),     # Riyadh local time
        F.lower(F.trim("status")).alias("status"),
        "_ingested_at",
        "_load_month",      # the bronze month this row came from (needed to sync deletes)
    )
    return keep_latest(out, ["order_id"]).withColumn("order_date", F.to_date("ordered_at"))

def clean_order_items(df):
    out = df.select(
        "order_id",
        F.col("line_no").cast("int").alias("line_no"),
        "product_id",
        F.col("quantity").cast("int").alias("quantity"),
        money("unit_price_sar").alias("unit_price_sar"),
        money("discount_sar").alias("discount_sar"),
        money("vat_sar").alias("vat_sar"),
        "_ingested_at",
        "_load_month",      # the bronze month this row came from (needed to sync deletes)
    )
    out = keep_latest(out, ["order_id", "line_no"])          # removes the duplicate lines
    net = (F.col("unit_price_sar") * F.col("quantity") - F.col("discount_sar")).cast("decimal(12,2)")
    return (out.withColumn("net_sales_sar", net)
               .withColumn("gross_sales_sar", (net + F.col("vat_sar")).cast("decimal(12,2)")))

def clean_payments(df):
    out = df.select(
        "order_id",
        F.col("payment_seq").cast("int").alias("payment_seq"),
        F.trim("payment_method").alias("payment_method"),
        F.col("installments").cast("int").alias("installments"),
        money("amount_sar").alias("amount_sar"),
        "_ingested_at",
        "_load_month",      # the bronze month this row came from (needed to sync deletes)
    )
    return (keep_latest(out, ["order_id", "payment_seq"])
            .withColumn("is_bnpl", F.col("payment_method").isin("Tabby", "Tamara")))

def clean_products(df, sector_map):
    return (df.join(sector_map.select("category", "sama_sector"), "category", "left")
              .select("product_id", "category", "sama_sector", "brand",
                      money("list_price_sar").alias("list_price_sar"))
              .dropDuplicates(["product_id"]))

def clean_customers(df):
    return (df.select("customer_id", standard_city("home_city").alias("home_city"))
              .dropDuplicates(["customer_id"]))

def clean_events(df):
    return df.select("event_name",
                     F.to_date("start_date").alias("start_date"),
                     F.to_date("end_date").alias("end_date"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## SAMA point-of-sale data cleaning

# CELL ********************

MARKET_COLUMNS = {           # bronze column name -> standard name
    "period": "period",      # the converted SAMA file already uses the standard names
    "frequency": "frequency",
    "sector": "sector",
    "indicator": "indicator",
    "value": "value",
}
MONTHLY = "Monthly"          # exact spellings as they appear in your file
SALES = "Sales"

def clean_market(df):
    # 1. Rename the columns to standard names
    for src, dst in MARKET_COLUMNS.items():
        df = df.withColumnRenamed(src, dst)

    # 2. Keep only monthly sales rows (drop quarterly, annual and transaction counts)
    df = df.filter(F.col("frequency") == MONTHLY)
    df = df.filter(F.col("indicator").contains(SALES))

    # 3. Build the three output columns
    month = F.trunc(F.to_date("period"), "month")             # e.g. 2024-01-01
    sector = F.trim("sector")                                 # remove stray spaces
    sales_sar = F.col("value").cast("double") * 1000          # thousands -> riyals
    return df.select(month.alias("month"), sector.alias("sector"),
                     sales_sar.alias("market_sales_sar"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Save helper (full overwrite, used for the small reference tables)

# CELL ********************

def save(df, name):
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").saveAsTable(name))
    print(f"{name}: {spark.table(name).count():,} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## One-time reset (only when RESET_SILVER = True)
# Drops the three silver sales tables and the bookmarks. The next cell then finds no bookmark and rebuilds
# silver from **all** of bronze in one batch.

# CELL ********************

if RESET_SILVER:
    for t in ["silver_orders", "silver_order_items", "silver_payments", "etl_watermark"]:
        spark.sql(f"DROP TABLE IF EXISTS {t}")
        print(f"dropped {t}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Upsert and watermark helpers

# CELL ********************

from delta.tables import DeltaTable

START = "1900-01-01 00:00:00"   # "never loaded": older than any real data


def upsert(df, target, keys, sync_months=None):
    """Update rows whose keys already exist in target, add rows whose keys are new.
    sync_months: the _load_month values that df contains in full. Rows of those months
    that are in target but missing from df were removed at the source, so they are deleted."""
    # First run: the table doesn't exist yet, so just create it
    if not spark.catalog.tableExists(target):
        df.write.format("delta").saveAsTable(target)
        return

    # Build the match rule, e.g. "t.order_id = s.order_id AND t.line_no = s.line_no"
    # t = the existing table, s = the new rows
    conditions = []
    for k in keys:
        conditions.append(f"t.{k} = s.{k}")
    condition = " AND ".join(conditions)

    target_table = DeltaTable.forName(spark, target)
    merge = (target_table.alias("t")
        .merge(df.alias("s"), condition)
        .whenMatchedUpdateAll()       # key found  -> overwrite that row
        .whenNotMatchedInsertAll())   # key is new -> add the row

    if sync_months:
        # Key in target but not in the batch -> delete it, but ONLY within the batch's months.
        # Other months are simply not in this batch; without this condition they would all be wiped.
        month_list = ", ".join(f"'{m}'" for m in sync_months)
        merge = merge.whenNotMatchedBySourceDelete(condition=f"t._load_month IN ({month_list})")

    merge.execute()


def get_watermark(table):
    """Return the newest timestamp already loaded for this table."""
    if not spark.catalog.tableExists("etl_watermark"):
        return START                                  # nothing loaded yet
    rows = spark.table("etl_watermark").filter(F.col("table_name") == table)
    row = rows.select("last_value").first()          # None if this table has no bookmark
    if row is None:
        return START
    return row["last_value"]


def set_watermark(table, value):
    """Save the new bookmark for this table (one row: table_name, last_value)."""
    df = spark.createDataFrame([(table, str(value))], "table_name string, last_value string")
    upsert(df, "etl_watermark", ["table_name"])   # replaces the old bookmark

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Load only new sales rows; rebuild the small reference tables

# CELL ********************

# ---- Part 1: sales tables (large, grow every month) -> load only what's new ----
# Each entry: (table name, cleaning function, key columns)
sales_tables = [
    ("orders",      clean_orders,      ["order_id"]),
    ("order_items", clean_order_items, ["order_id", "line_no"]),
    ("payments",    clean_payments,    ["order_id", "payment_seq"]),
]

for name, clean_fn, keys in sales_tables:
    bronze_name = f"bronze_{name}"
    silver_name = f"silver_{name}"

    # 1. Read the bookmark: when did we last load this table?
    last_load = get_watermark(silver_name)

    # 2. Keep only bronze rows ingested after the bookmark (one month, or many after a backfill)
    bronze = spark.table(bronze_name)
    new_rows = bronze.filter(F.col("_ingested_at") > F.lit(last_load).cast("timestamp"))

    if new_rows.isEmpty():
        print(f"{silver_name}: nothing new since {last_load}")
        continue                                  # skip to the next table

    # 3. The newest load time in this batch becomes the new bookmark
    newest = new_rows.agg(F.max("_ingested_at")).first()[0]

    # 4. Clean the new rows and sync them into silver: update, insert, and delete
    #    rows removed at the source. Bronze always reloads a whole month, so the batch
    #    holds every row of each month in it.
    batch_months = sorted(r["_load_month"] for r in new_rows.select("_load_month").distinct().collect())
    upsert(clean_fn(new_rows), silver_name, keys, sync_months=batch_months)

    # 5. Move the bookmark forward only after the merge succeeded
    set_watermark(silver_name, newest)
    print(f"{silver_name}: merged {len(batch_months)} month(s) {batch_months[0]} to {batch_months[-1]}")

# ---- Part 2: reference tables (small) -> rebuild in full every run ----
products = clean_products(spark.table("bronze_products"), spark.table("bronze_category_sector_map"))
save(products, "silver_products")
save(clean_customers(spark.table("bronze_customers")), "silver_customers")
save(spark.table("bronze_branches").select("branch_id", "city", "region"), "silver_branches")
save(clean_events(spark.table("bronze_events")), "silver_events")
save(clean_market(spark.table("bronze_market_pos")), "silver_market_monthly")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Reconciliation check
# For every month, silver must have as many rows as bronze has distinct keys. A mismatch stops the run,
# so the pipeline's failure alert fires.

# CELL ********************

checks = [("orders", "order_id"),
          ("order_items", "order_id, line_no"),
          ("payments", "order_id, payment_seq")]

problems = []
for name, key in checks:
    diff = spark.sql(f"""
        SELECT COALESCE(b.m, s.m) AS load_month, b.n AS bronze_keys, s.n AS silver_rows
        FROM (SELECT _load_month AS m, COUNT(DISTINCT {key}) AS n
              FROM bronze_{name} GROUP BY _load_month) b
        FULL JOIN (SELECT _load_month AS m, COUNT(*) AS n
                   FROM silver_{name} GROUP BY _load_month) s
          ON b.m = s.m
        WHERE b.n IS NULL OR s.n IS NULL OR b.n <> s.n
    """)
    if diff.isEmpty():
        print(f"{name}: bronze and silver match for every month")
    else:
        print(f"{name}: MISMATCH")
        diff.show()
        problems.append(name)

if problems:
    raise Exception(f"Silver does not match bronze for: {problems}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Quick look (optional, safe to keep in the pipeline)

# CELL ********************

spark.sql("""
    SELECT _load_month, COUNT(*) AS orders, SUM(CASE WHEN ordered_at IS NULL THEN 1 ELSE 0 END) AS unparsed_times
    FROM silver_orders GROUP BY _load_month ORDER BY _load_month
""").show(50)
spark.sql("SELECT city, COUNT(*) AS orders FROM silver_orders GROUP BY city ORDER BY orders DESC").show(20)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
