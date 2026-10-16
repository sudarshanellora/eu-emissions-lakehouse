# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze ingest – Eurostat
# MAGIC Downloads raw Eurostat datasets to a UC volume and lands them as bronze Delta tables.
# MAGIC
# MAGIC | Dataset | Description |
# MAGIC |---|---|
# MAGIC | `env_ac_ainah_r2` | Air emissions accounts by NACE Rev. 2 activity |
# MAGIC | `nama_10_a64` | National accounts aggregates by industry (A*64) |
# MAGIC
# MAGIC Bronze = raw content, wide format, plus ingestion metadata. No business logic here.
# MAGIC If the download step fails (outbound network blocked), download the files manually
# MAGIC from the URLs printed below, upload them to the volume, and set `download = false`.

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("project", "emissions", "Schema prefix")
dbutils.widgets.dropdown("download", "true", ["true", "false"], "Download from API")

CATALOG = dbutils.widgets.get("catalog")
PREFIX = dbutils.widgets.get("project")
DOWNLOAD = dbutils.widgets.get("download") == "true"

VOLUME_PATH = f"/Volumes/{CATALOG}/{PREFIX}_landing/raw"
BRONZE_SCHEMA = f"{CATALOG}.{PREFIX}_bronze"

API = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/{code}?format=TSV&compressed=true"

DATASETS = {
    "env_ac_ainah_r2": "eurostat_air_emissions_raw",
    "nama_10_a64": "eurostat_gva_raw",
}

for code in DATASETS:
    print(f"{code}: {API.format(code=code)}  ->  {VOLUME_PATH}/{code}.tsv.gz")

# COMMAND ----------

# MAGIC %md ## 1. Download to volume

# COMMAND ----------

import os
import requests


def download(code: str) -> str:
    """Stream a Eurostat dataset to the volume. Returns the file path."""
    target = f"{VOLUME_PATH}/{code}.tsv.gz"
    with requests.get(API.format(code=code), stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(target, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)
    print(f"Downloaded {code}: {os.path.getsize(target) / 1e6:.1f} MB")
    return target


if DOWNLOAD:
    for code in DATASETS:
        try:
            download(code)
        except Exception as e:
            print(f"Download failed for {code}: {e}\n"
                  f"Upload the file manually to {VOLUME_PATH}/{code}.tsv.gz and re-run with download=false.")
else:
    print("Skipping download; using files already in the volume.")

display(dbutils.fs.ls(VOLUME_PATH))

# COMMAND ----------

# MAGIC %md ## 2. Land as bronze Delta
# MAGIC Eurostat TSV header looks like `freq,airpol,nace_r2,unit,geo\TIME_PERIOD <tab> 2008 <tab> 2009 ...`.
# MAGIC Delta does not allow `,` or `\` in column names, so the key column becomes `series_key`
# MAGIC (the original header is stored in `_key_dims`) and year columns become `y_2008` etc.
# MAGIC Values stay as strings (they carry flags like `123.4 p` and `:`); parsing happens in silver.

# COMMAND ----------

import re
from pyspark.sql import functions as F


def to_bronze(code: str, table: str) -> None:
    path = f"{VOLUME_PATH}/{code}.tsv.gz"
    df = (spark.read
          .option("sep", "\t")
          .option("header", True)
          .option("inferSchema", False)
          .csv(path))

    key_col = df.columns[0]                      # e.g. "freq,airpol,nace_r2,unit,geo\TIME_PERIOD"
    key_dims = key_col.split("\\")[0].strip()    # "freq,airpol,nace_r2,unit,geo"

    renamed = {key_col: "series_key"}
    for c in df.columns[1:]:
        clean = c.strip()
        renamed[c] = f"y_{clean}" if re.fullmatch(r"\d{4}", clean) else re.sub(r"\W", "_", clean)

    df = df.select([F.col(f"`{old}`").alias(new) for old, new in renamed.items()])
    df = (df
          .withColumn("_dataset", F.lit(code))
          .withColumn("_key_dims", F.lit(key_dims))
          .withColumn("_source_file", F.lit(path))
          .withColumn("_ingested_at", F.current_timestamp()))

    full_name = f"{BRONZE_SCHEMA}.{table}"
    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", True)
       .saveAsTable(full_name))

    spark.sql(f"COMMENT ON TABLE {full_name} IS 'Raw Eurostat {code} (wide TSV). Key dims: {key_dims}'")
    print(f"{full_name}: {spark.table(full_name).count():,} rows, {len(df.columns)} columns")


for code, table in DATASETS.items():
    to_bronze(code, table)

# COMMAND ----------

# MAGIC %md ## 3. Quick checks

# COMMAND ----------

for table in DATASETS.values():
    t = spark.table(f"{BRONZE_SCHEMA}.{table}")
    years = sorted(c for c in t.columns if c.startswith("y_"))
    print(f"{table}: years {years[0][2:]}–{years[-1][2:]}, key dims = {t.first()['_key_dims']}")
    display(t.limit(5))
