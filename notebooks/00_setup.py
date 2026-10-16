# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup
# MAGIC Creates the medallion schemas and a Unity Catalog volume for raw files.
# MAGIC Run once. Safe to re-run.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("project", "emissions", "Schema prefix")

CATALOG = dbutils.widgets.get("catalog")
PREFIX = dbutils.widgets.get("project")

SCHEMAS = {
    "landing": f"{PREFIX}_landing",  # holds the raw-files volume
    "bronze": f"{PREFIX}_bronze",
    "silver": f"{PREFIX}_silver",
    "gold": f"{PREFIX}_gold",
}

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")

for layer, schema in SCHEMAS.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema} COMMENT '{layer} layer – EU emissions lakehouse'")

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMAS['landing']}.raw COMMENT 'Raw Eurostat downloads'")

display(spark.sql(f"SHOW SCHEMAS IN {CATALOG} LIKE '{PREFIX}_*'"))
print(f"Raw volume path: /Volumes/{CATALOG}/{SCHEMAS['landing']}/raw")
