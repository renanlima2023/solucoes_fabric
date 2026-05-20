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
from delta.tables import DeltaTable
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
print(fabric.resolve_workspace_name())

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

# Isola o nome final da tabela e força a criação no catálogo raiz (default)
nome_limpo = target_table.split(".")[-1]
tabela_nome = f"default.{nome_limpo}"

# Verifica se a tabela já existe registrada no catálogo do Lakehouse
tabela_existe = spark.catalog.tableExists(tabela_nome)

if not tabela_existe:
    print(f"Criando tabela identificada no Lakehouse: {tabela_nome}...")
    df_final.write.format("delta").mode("overwrite").saveAsTable(tabela_nome)

else:
    print(f"Fazendo MERGE na tabela identificada {tabela_nome}...")

    # Instancia a tabela Delta usando o nome mapeado no catálogo
    delta_table = DeltaTable.forName(spark, tabela_nome)

    delta_table.alias("t").merge(
        df_final.alias("s"), f"t.{target_key} = s.{target_key}"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
