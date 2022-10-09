## My repo for the Azure Databricks with Pyspark/SQL course on Udemy

This repository contains my hands on code and exercise solutions for the [Azure Databricks & Spark Core For Data Engineers(Python/SQL)](https://www.udemy.com/course/azure-databricks-spark-core-for-data-engineers/) course on Udemy.

### Instructions

Although these are `.py` and `.sql` files, these are Databricks notebooks, and you could work with them by either of the following ways:
* Zip the required folders (excluding `README.md` and `LICENSE.txt` as these are incompatible file formats) and import the zip in your Databricks workspace, OR
* Work with them in your preferred and compatible IDE/editor with the help of [dbx](https://docs.databricks.com/dev-tools/index-ide.html) or [Databricks Connect](https://docs.databricks.com/dev-tools/databricks-connect.html)

Please note that throughout the code, I've used my ADLS storage account's name - `formula1deltake`. You will have to create your own ADLS storage account and replace `formula1deltake` with its name. You'll also have to replace your storage account's `client_id`, `tenant_id` and `client_secret` in [mount_adls_storage.py](setup/mount_adls_storage.py) and run it once in order to mount your storage account to Databricks.

### License

GNU General Public License v3.0 or later

See [LICENSE.txt](LICENSE.txt) to see the full text.
