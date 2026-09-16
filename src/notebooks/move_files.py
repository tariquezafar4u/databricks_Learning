# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # File Mover Notebook (Event Triggered)
# MAGIC Reads file arrival event params and copies/moves files from source volume to target volume.

# COMMAND ----------
import os
import sys

# COMMAND ----------
# Extract widget parameters passed by the Databricks Job
dbutils.widgets.text("source_path", "", "Source Volume Path")
dbutils.widgets.text("target_path", "", "Target Volume Path")
dbutils.widgets.text("trigger_file_path", "", "Event Trigger File Path")

source_path = dbutils.widgets.get("source_path").strip()
target_path = dbutils.widgets.get("target_path").strip()
trigger_file_path = dbutils.widgets.get("trigger_file_path").strip()

print(f"Source Volume Path : {source_path}")
print(f"Target Volume Path : {target_path}")
print(f"Trigger File Path  : {trigger_file_path}")

# COMMAND ----------
if not source_path or not target_path:
    raise ValueError("Both source_path and target_path must be configured!")

# If triggered by a specific file arrival event, process only that file
if trigger_file_path and trigger_file_path != "{{event.file_arrival.url}}":
    file_name = os.path.basename(trigger_file_path)
    dest_file_path = f"{target_path.rstrip('/')}/{file_name}"
    
    print(f"Copying incoming triggered file: {trigger_file_path} -> {dest_file_path}")
    dbutils.fs.cp(trigger_file_path, dest_file_path)
    print(f" Successfully copied {file_name} to target volume.")
else:
    # Manual or full directory copy
    print(f"Scanning source directory: {source_path}")
    files = dbutils.fs.ls(source_path)
    
    if not files:
        print("No files found in source location.")
    else:
        for f in files:
            if not f.isDir():
                dest_file_path = f"{target_path.rstrip('/')}/{f.name}"
                print(f"Copying {f.path} -> {dest_file_path}")
                dbutils.fs.cp(f.path, dest_file_path)
                print(f" Successfully copied {f.name}")

print("All file transfers completed successfully.")
