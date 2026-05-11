# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

spark.conf.set("spark.sql.caseSensitive", True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import sempy.fabric as fabric
workspace_id = fabric.get_workspace_id()
workspace_name = fabric.resolve_workspace_name()
print(fabric.resolve_workspace_name())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Paramentros

source_storage= ""
source_folder= ""
source_file= ""

target_storage= ""
target_table= ""
target_mode= ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Caminho abs para a orgirem (arquivos parquet)
path_staging= (
    f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/"
    f"{source_storage}.Lakehouse/Files/{source_folder}/{source_file}"
)

# Caminho abs paraa o destino (tabelas Delta)
path_bronze = (f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/"
f"{target_storage}.Lakehouse/Tables/{target_table}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Leitura do Arquivo Parquet na Staging
df = spark.read.parquet(
    f"Files/{source_folder}/{source_file}"
)

# Escrita da Tabela Delta na camada Bronze
df.write.format("delta") \
    .mode(target_mode) \
    .saveAsTable(f"dbo.{target_table}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
