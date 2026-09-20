# Databricks notebook source
"""Load files from the landing zone into a raw Delta table as-is."""

from pyspark.sql import functions as F

dbutils.widgets.text("source_path", "")
dbutils.widgets.text("raw_table", "")
dbutils.widgets.text("file_format", "csv")
dbutils.widgets.text("write_mode", "append")

source_path = dbutils.widgets.get("source_path").rstrip("/")
raw_table = dbutils.widgets.get("raw_table")
file_format = dbutils.widgets.get("file_format").lower()
write_mode = dbutils.widgets.get("write_mode").lower()


if not source_path or not raw_table:
    raise ValueError("source_path and raw_table are required")

files = [f for f in dbutils.fs.ls(source_path) if not f.isDir()]
if not files:
    dbutils.notebook.exit("NO_FILES")

reader = spark.read.format(file_format)
if file_format == "csv":
    # Everything stays string so the raw layer is a faithful copy of the file.
    reader = reader.option("header", "true").option("inferSchema", "false")

df = (
    reader.load(source_path)
    .withColumn("__source_file", F.col("_metadata.file_path"))
    .withColumn("__ingested_at", F.current_timestamp())
)

(
    df.write.format("delta")
    .mode(write_mode)
    .option("mergeSchema", "true")
      .saveAsTable(raw_table)
)

dbutils.notebook.exit(f"LOADED {df.count()} rows into {raw_table}")