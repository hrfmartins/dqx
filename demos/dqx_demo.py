# Databricks notebook source
# MAGIC %md
# MAGIC ## Install DQX

# COMMAND ----------

# MAGIC %md
# MAGIC ### Installation DQX in the workspace
# MAGIC
# MAGIC Install DQX in the workspace as per the instructions [here](https://github.com/databrickslabs/dqx?tab=readme-ov-file#installation).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Install DQX from a wheel file in the current cluster

# COMMAND ----------

import subprocess

user_name = spark.sql('select current_user() as user').collect()[0]['user']
pip_install_path = f"/Workspace/Users/{user_name}/.dqx/wheels/databricks_labs_dqx-*.whl"
%pip install {pip_install_path}

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate quality rule candidates using Profiler

# COMMAND ----------

from databricks.labs.dqx.profiler.profiler import profile

schema = "col1: int, col2: int, col3: int, col4 int"
input_df = spark.createDataFrame([[1, 3, 3, 1], [2, None, 4, 1]], schema)

summary_stats, checks = profile(input_df)
display(summary_stats)
display(checks)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply quality rules using yaml-like dictionary

# COMMAND ----------

import yaml
from databricks.labs.dqx.engine import apply_checks_by_metadata, apply_checks_by_metadata_and_split


checks = yaml.safe_load("""
- criticality: "error"
  check:
    function: "is_not_null"
    arguments:
      col_names:
        - "col1"
        - "col2"

- criticality: "error"
  check:
    function: "is_not_null_and_not_empty"
    arguments:
      col_name: "col3"

- criticality: "warn"
  check:
    function: "value_is_in_list"
    arguments:
      col_name: "col4"
      allowed:
        - 1
        - 2
""")

schema = "col1: int, col2: int, col3: int, col4 int"
input_df = spark.createDataFrame([[1, 3, 3, 1], [2, None, 4, 1]], schema)

# Option 1: apply quality rules on the dataframe and provide valid and invalid (quarantined) dataframes 
#valid_df, quarantined_df = apply_checks_by_metadata_and_split(input_df, checks)

# Option 2: apply quality rules on the dataframe and report issues as additional columns (`_warning` and `_error`)
valid_and_quarantined_df = apply_checks_by_metadata(input_df, checks)
display(valid_and_quarantined_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply quality rules using DQX classes

# COMMAND ----------

from databricks.labs.dqx.col_functions import is_not_null, is_not_null_and_not_empty, value_is_in_list
from databricks.labs.dqx.engine import DQRule, DQRuleColSet, apply_checks, apply_checks_and_split


checks = DQRuleColSet( # define rule for multiple columns at once
            columns=["col1", "col2"], 
            criticality="error", 
            check_func=is_not_null).get_rules() + [
         DQRule( # define rule for a single column
            name='col3_is_null_or_empty',
            criticality='error', 
            check=is_not_null_and_not_empty('col3')),
         DQRule( # name auto-generated if not provided       
            criticality='warn', 
            check=value_is_in_list('col4', ['1', '2']))
        ]

schema = "col1: int, col2: int, col3: int, col4 int"
input_df = spark.createDataFrame([[1, 3, 3, 1], [2, None, 4, 1]], schema)

# Option 1: apply quality rules on the dataframe and provide valid and invalid (quarantined) dataframes
#valid_df, quarantined_df = apply_checks_and_split(input_df, checks)

# Option 2: apply quality rules on the dataframe and report issues as additional columns (`_warning` and `_error`)
valid_and_quarantined_df = apply_checks(input_df, checks)

display(valid_and_quarantined_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply quality rules from checks stored in a workspace file

# COMMAND ----------

# MAGIC %md
# MAGIC ### Save checks to a default location

# COMMAND ----------

# store checks in a workspace file

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat


data = yaml.safe_load("""
- criticality: error
  check:
    function: is_not_null
    arguments:
      col_names:
      - col1
      - col2
- name: col_col3_is_null_or_empty
  criticality: error
  check:
    function: is_not_null_and_not_empty
    arguments:
      col_name: col3
- criticality: warn
  check:
    function: value_is_in_list
    arguments:
      col_name: col4
      allowed:
      - 1
      - 2
""")

# Define the local filename and workspace path
local_file_path = "/tmp/checks.yml"
user_name = spark.sql('select current_user() as user').collect()[0]['user']
workspace_file_path = f"/Workspace/Users/{user_name}/.dqx/checks.yml"

# Save the YAML content to a local file
with open(local_file_path, "w") as file:
    yaml.dump(data, file)

# Upload the file to Databricks workspace
ws = WorkspaceClient()
print(f"Uploading checks to {workspace_file_path}")
with open(local_file_path, "rb") as file:
    raw = file.read()
ws.workspace.upload(workspace_file_path, raw, format=ImportFormat.AUTO, overwrite=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply quality rules

# COMMAND ----------

from databricks.labs.dqx.engine import apply_checks_by_metadata, apply_checks_by_metadata_and_split
from databricks.labs.dqx.engine import load_checks_from_file
from databricks.sdk import WorkspaceClient
from databricks.labs.blueprint.installation import Installation

# use check file specified in the default installation config ('config.yml')
# if filename provided it's a relative path to the workspace installation directory
ws = WorkspaceClient()
installation = Installation.current(ws, "dqx", assume_user=True)
checks = load_checks_from_file(installation)
print(checks)
# Option 2: apply quality rules on the dataframe and report issues as additional columns (`_warning` and `_error`)
valid_and_quarantined_df = apply_checks_by_metadata(input_df, checks)
display(valid_and_quarantined_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Apply checks in the medalion architecture

# COMMAND ----------

# Prepare bronze layer
bronze_path = "/tmp/dqx_demo/bronze"
df = spark.read.format("delta").load("/databricks-datasets/delta-sharing/samples/nyctaxi_2019")
df.write.format("delta").mode("overwrite").save(bronze_path)

# COMMAND ----------

# Define our Data Quality cheks
import yaml


checks = yaml.safe_load("""
- check:
    function: "is_not_null"
    arguments:
      col_names:
        - "vendor_id"
        - "pickup_datetime"
        - "dropoff_datetime"
        - "passenger_count"
  criticality: "error"

- check:
    function: "is_not_null_and_not_empty"
    arguments:
      col_name: "vendor_id"
      trim_strings: true
  name: "vendor_id_is_null_or_empty"
  criticality: "error"

- check:
    function: "is_in_range"
    arguments:
      col_name: "pickup_datetime"
      min_limit: "2019-01-01T00:00:00.000000"
      max_limit: "2020-01-01T00:00:00.000000"
  name: "pickup_datetime_isnt_in_range"
  criticality: "warn"

- check:
    function: "is_in_range"
    arguments:
      col_name: "dropoff_datetime"
      min_limit: "2019-01-01T00:00:00.000000"
      max_limit: "2020-01-02T00:00:00.000000"
  name: "dropoff_datetime_isnt_in_range"
  criticality: "warn"

- check:
    function: "is_in_range"
    arguments:
      col_name: "passenger_count"
      min_limit: 0
      max_limit: 6
  name: "passenger_incorrect_count"
  criticality: "warn"

- check:
    function: "is_not_null"
    arguments:
      col_name: "trip_distance"
  name: "trip_distance_is_null"
  criticality: "error"
""")

# COMMAND ----------

from databricks.labs.dqx.engine import apply_checks_by_metadata_and_split

# Apply checks when processing to silver layer
bronze = spark.read.format("delta").load(bronze_path)
silver, quarantine = apply_checks_by_metadata_and_split(bronze, checks)

# COMMAND ----------

display(silver)

# COMMAND ----------

display(quarantine)
