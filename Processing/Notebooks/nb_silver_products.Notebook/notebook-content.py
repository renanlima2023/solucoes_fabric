# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

%run nd_functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Configurações da sessão spark
spark.conf.set("spark.sql.CaseSensitive", True)

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

df_product= (
    df["Product"]
    .select(
        "ProductID",
        F.col("Name").alias("ProductName"),
        "Color",
        "StandardCost",
        "ListPrice",
        "Size",
        "Weight",
        "ProductCategoryID",
        "ProductModelID",
        F.to_date("SellStartDate").alias("SellStartDate"),
        F.to_date("SellEndDate").alias("SellEndDate"),
        F.to_date("DiscontinuedDate").alias("DiscontinuedDate"),
      F.to_date ("ModifiedDate").alias("ProductModifiedDate"))
  )

df_category = (
    df["ProductCategory"]
    .select(
        "ProductCategoryID",
        "ParentProductCategoryID",
        F.col("Name").alias("CategoryName"),
    F.to_date("ModifiedDate").alias("CategoryModifiedDate")
    )
  )

df_model =(
    df["ProductModel"]
    .select(
        "ProductModelID",
        F.col("Name").alias("ModelName"),
        F.to_date("ModifiedDate").alias("ModelModifiedDate")
    )
   
)

df_description =(
    df["ProductDescription"]
    .select(
        "ProductDescriptionID",
        "Description",
        F.to_date("ModifiedDate").alias("DescriptionModifiedDate")
    )
)

df_bridge = (
    df["ProductModelProductDescription"]
    .select(
        "ProductModelID",
        "ProductDescriptionID",
        F.to_date("ModifiedDate").alias("BridgeModifiedDate")
    )
    .where(F.trim("Culture")== "en")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_final = (
    df_product
.join(df_category, "ProductCategoryID", how= "left")
.join(df_model, "ProductModelID", how= "left")
.join(df_bridge, "ProductModelID", how= "left")
.join(df_description, "ProductDescriptionID", how= "left")
)

df_final = (
    df_final
    .withColumn("ModifiedDate", F.greatest(
        F.col("ProductModifiedDate"),  
        F.col("CategoryModifiedDate"),
        F.col("ModelModifiedDate"),
        F.col("DescriptionModifiedDate"),
        F.col("BridgeModifiedDate") )
    )
     .withColumn("UpdatedDate", F.current_date())
)

df_final = (
    df_final
    .drop(
        "ProductDescriptionID",
        "ProductModelID",
        "ProductModifiedDate",
        "CategorydifiedDate",
        "ModeldifiedDate",
        "DescriptionfiedDate",
        "BridgeModifiedDate")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Gravação da Tabela com o modo atribuido
if target_mode == "merge":
    safe_merge(df_final, path_silver, target_key)
else:
    df_final.write.format("delta").mode(target_mode).save(path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
