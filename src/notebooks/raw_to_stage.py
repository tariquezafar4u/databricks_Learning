# Databricks notebook source
"""Validate raw rows (type cast, not-null, trim, primary key) and load into the stage Delta table."""

import json
from pyspark.sql import functions as F, Window
from pyspark.sql.types import StringType

dbutils.widgets.text("raw_table", "")
dbutils.widgets.text("stage_table", "")
dbutils.widgets.text("reject_table", "")
dbutils.widgets.text("expected_schema",('{"job": "string"}') )
dbutils.widgets.text("primary_keys", "empno")      # comma-separated, e.g. "employee_id"
dbutils.widgets.text("not_null_columns", "deptno")  # comma-separated, PK is added automatically
dbutils.widgets.text("write_mode", "append")
dbutils.widgets.text("external_location", "")

raw_table = dbutils.widgets.get("raw_table")
stage_table = dbutils.widgets.get("stage_table")
reject_table = dbutils.widgets.get("reject_table")
expected_schema = json.loads(dbutils.widgets.get("expected_schema") or "{}")
primary_keys = [c.strip() for c in dbutils.widgets.get("primary_keys").split(",") if c.strip()]
not_null_columns = [c.strip() for c in dbutils.widgets.get("not_null_columns").split(",") if c.strip()]
write_mode = dbutils.widgets.get("write_mode").lower()
external_location = dbutils.widgets.get("external_location")

if not raw_table or not stage_table or not expected_schema or not primary_keys:
    raise ValueError("raw_table, stage_table, expected_schema and primary_keys are required")

not_null_columns = sorted(set(not_null_columns) | set(primary_keys))

df = spark.table(raw_table)

# Trim string columns before validation and stage-table ingestion.
string_columns = [
    field.name
    for field in df.schema.fields
    if isinstance(field.dataType, StringType)
]

for column_name in string_columns:
    df = df.withColumn(
        column_name,
        F.trim(F.col(column_name))
    )

#1. Trim every string column so whitespace never breaks equality/PK checks downstream.
string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
for c in string_cols:
    df = df.withColumn(c, F.trim(F.col(c)))

reasons = []

#2. Data type check: try_cast keeps a nullable typed value instead of failing the job,
#so "was non-null before, null after" is exactly a cast failure.
for col_name, data_type in expected_schema.items():
    raw_col = f"__raw_{col_name}"
    fail_flag = f"__dtype_fail_{col_name}"
    df = (
        df.withColumn(raw_col, F.col(col_name))
        .withColumn(col_name, F.expr(f"try_cast(`{col_name}` as {data_type})"))
        .withColumn(fail_flag, F.col(raw_col).isNotNull() & F.col(col_name).isNull())
        .drop(raw_col)
    )
    reasons.append(F.when(F.col(fail_flag), F.lit(f"invalid_type:{col_name}")))

#3. Not-null check (primary key columns are always included).
for c in not_null_columns:
    fail_flag = f"__notnull_fail_{c}"
    df = df.withColumn(fail_flag, F.col(c).isNull())
    reasons.append(F.when(F.col(fail_flag), F.lit(f"null_value:{c}")))

#4. Primary key validation: duplicates across the whole batch, not just within one file.
pk_row_count = F.count("*").over(Window.partitionBy(*primary_keys))
df = df.withColumn("__pk_row_count", pk_row_count)
df = df.withColumn("__pk_duplicate_fail", F.col("__pk_row_count") > 1)
reasons.append(F.when(F.col("__pk_duplicate_fail"), F.lit("duplicate_primary_key")))

df = df.withColumn("__reject_reason", F.concat_ws(";", *reasons))
df = df.withColumn("__is_valid", F.length("__reject_reason") == 0)

fail_cols = [c for c in df.columns if c.startswith("__dtype_fail_") or c.startswith("__notnull_fail_")]
drop_common = ["__pk_row_count", "__pk_duplicate_fail", *fail_cols]

valid_df = df.filter("__is_valid").drop("__is_valid", "__reject_reason", *drop_common)
reject_df = (
    df.filter("NOT __is_valid")
    .withColumn("__rejected_at", F.current_timestamp())
    .drop("__is_valid", *drop_common)
)

(
    valid_df.write.format("delta")
    .mode(write_mode)
    .option("mergeSchema", "true")
    .option("path", f"{external_location}/{stage_table}")
    .saveAsTable(stage_table)
)

if reject_table:
    (
        reject_df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .option("path", f"{external_location}/{reject_table}")
        .saveAsTable(reject_table)
    )

dbutils.notebook.exit(
    f"STAGE_LOADED valid={valid_df.count()} rejected={reject_df.count()} into {stage_table}"
)