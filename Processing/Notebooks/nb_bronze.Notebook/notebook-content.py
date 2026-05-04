# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "171fd0c6-8801-4fbc-aa81-c969dc4d80ec",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "51267455-e79f-4f83-a4a9-117ad1f1554b",
# META       "known_lakehouses": [
# META         {
# META           "id": "171fd0c6-8801-4fbc-aa81-c969dc4d80ec"
# META         }
# META       ]
# META     }
# META   }
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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

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
df = spark.read.parquet(path_staging)
# Escrita da Tabela Delta na camada Bronze
df.write.format("delta").mode(target_mode).save(path_bronze)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
