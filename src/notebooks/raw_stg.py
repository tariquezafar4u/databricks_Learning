# Databricks notebook source
"""Validate raw rows (not-null, trim) and load into the stage Delta table."""

from pyspark.sql import functions as F
from pyspark.sql.types import StringType

dbutils.widgets.text("raw_table", "")
dbutils.widgets.text("stage_table", "")
dbutils.widgets.text("reject_table", "")
dbutils.widgets.text("not_null_columns", "age")  # comma-separated, e.g. "employee_id,name"
dbutils.widgets.text("write_mode", "append")

raw_table = dbutils.widgets.get("raw_table")
stage_table = dbutils.widgets.get("stage_table")
reject_table = dbutils.widgets.get("reject_table")
not_null_columns = [c.strip() for c in dbutils.widgets.get("not_null_columns").split(",") if c.strip()]
write_mode = dbutils.widgets.get("write_mode").lower()

if not raw_table or not stage_table or not not_null_columns:
    raise ValueError("raw_table, stage_table and not_null_columns are required")

df = spark.table(raw_table)

#1. Trim every string column so whitespace never hides as a valid value.
string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
for c in string_cols:
    df = df.withColumn(c, F.trim(F.col(c)))

reasons = []

#2. Not-null check on the required columns.
for c in not_null_columns:
    fail_flag = f"__notnull_fail_{c}"
    df = df.withColumn(fail_flag, F.col(c).isNull())
    reasons.append(F.when(F.col(fail_flag), F.lit(f"null_value:{c}")))

df = df.withColumn("__reject_reason", F.concat_ws(";", *reasons))
df = df.withColumn("__is_valid", F.length("__reject_reason") == 0)

fail_cols = [c for c in df.columns if c.startswith("__notnull_fail_")]

valid_df = df.filter("__is_valid").drop("__is_valid", "__reject_reason", *fail_cols)
reject_df = (
    df.filter("NOT __is_valid")
    .withColumn("__rejected_at", F.current_timestamp())
    .drop("__is_valid", *fail_cols)
)

(
    valid_df.write.format("delta")
    .mode(write_mode)
    .option("mergeSchema", "true")
    .saveAsTable(stage_table)
)

if reject_table:
    (
        reject_df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(reject_table)
    )

dbutils.notebook.exit(
    f"STAGE_LOADED valid={valid_df.count()} rejected={reject_df.count()} into {stage_table}"
)