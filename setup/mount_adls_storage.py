# Databricks notebook source
storage_account_name = "formula1deltake"
client_id = "client_id_here"
tenant_id = "tenant_id_here"
client_secret = "client_secret_here"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

def mount_adls(container_name):
  dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

# COMMAND ----------

mount_adls("presentation")

# COMMAND ----------

mount_adls("processed")

# COMMAND ----------

mount_adls("raw")

# COMMAND ----------

mount_adls("demo")

# COMMAND ----------

dbutils.fs.ls(f"/mnt/{storage_account_name}/raw")

# COMMAND ----------

dbutils.fs.ls(f"/mnt/{storage_account_name}/processed")

# COMMAND ----------

