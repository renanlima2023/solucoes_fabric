# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "979d0bea-388a-4223-930c-1ce1f3c2a34d",
# META       "default_lakehouse_name": "lh_silver",
# META       "default_lakehouse_workspace_id": "158b5a59-9912-49d3-8467-c01f1a4c032b",
# META       "known_lakehouses": [
# META         {
# META           "id": "979d0bea-388a-4223-930c-1ce1f3c2a34d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

%run nd_functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.caseSensitive", True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Bibliotecas
from pyspark.sql import functions as F
import sempy.fabric as fabric


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Obtenção do workspaceID e workspaceName
workspace_id = fabric.get_notebook_workspace_id()
workspace_name = fabric.resolve_workspace_name()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(workspace_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

#Paramentros
source_storage = ""
source_tables = ""

target_storage= ""
target_table = ""
target_mode = ""

target_key = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Caminho absuloto para o destino
path_silver = (f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/"
f"{target_storage}.Lakehouse/Tables/{target_table}"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Leitura das Tabelas
source_tables = source_tables.split("|")

df={}

for table in source_tables:
    path_bronze = (f"abfss://{workspace_name}@onelake.dfs.fabric.microsoft.com/"
    f"{source_storage}.Lakehouse/Tables/{table}"
    )
    df[table] = spark.read.format("delta").load(path_bronze)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_address = (
    df['Address']
    .select(
        "AddressID",
        "AddressLine1",
        "AddressLine2",
        "City",
        "StateProvince",
        "CountryRegion",
        "PostalCode",
      F.to_date ("ModifiedDate").alias("AddressModifiedDate"))
  )

df_customer = (
    df["Customer"]
    .select(
        "CustomerID",
        "Title",
        "FirstName",
        "MiddleName",
        "LastName",
        "Suffix",
        "CompanyName",
    F.to_date("ModifiedDate").alias("CustomerModifiedDate")
    )
  )

df_bridge =(
    df["CustomerAddress"]
    .select(
        "CustomerID",
        "AddressID",
        F.to_date("ModifiedDate").alias("BridgeModifiedDate")
    )
    .where(F.col("AddressType") == "Main office")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final = (df_customer
.join(df_bridge, "CustomerID", how= "left")
.join(df_address, "AddressID", how= "left")
)

df_final = (
    df_final
    .withColumn
("ModifiedDate", F.greatest(
        F.col("AddressModifiedDate"),  
        F.col("CustomerModifiedDate"),
        F.col("BridgeModifiedDate") )
    )
     .withColumn("UpdatedDate", F.current_date())
)

df_final = (
    df_final
    .drop(
        "AddressID",
        "AddressModifiedDate",
        "CustomerModifiedDate",
        "BridgeModifiedDate")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

tabela_existe = spark._jsparkSession.catalog().tableExists(target_table)

if not tabela_existe:
    print(f"Criando tabela {target_table} com saveAsTable()...")
    df_final.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(target_table)

else:
    print(f"Fazendo MERGE na tabela {target_table}...")
    safe_merge(df_final, path_silver, target_key)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
