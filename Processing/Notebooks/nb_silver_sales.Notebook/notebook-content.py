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
from pyspark.sql.window import Window  # Alterado para importar diretamente Window (sem alias W)
from delta.tables import DeltaTable    # Import necessário para o Merge
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

df_sales_orders= (
    df["SalesOrderHeader"]
    .select(
        "SalesOrderID",
        "CustomerID",
        "ShipToAddressID",
        "BillToAddressID",
        F.to_date("OrderDate").alias("OrderDate"),
        F.to_date("DueDate").alias("DueDate"),
        F.to_date("ShipDate").alias("ShipDate"),
        "SalesOrderNumber",
        "PurchaseOrderNumber",
        "AccountNumber",
        "SubTotal",
        "TaxAmt",
        "Freight",
        "TotalDue",
      F.to_date ("ModifiedDate").alias("OrderModifiedDate"))
  )

df_sales_details = (
    df["SalesOrderDetail"]
    .select(
        "SalesOrderID",
        "SalesOrderDetailID",
        "ProductID",
        "OrderQty",
        "UnitPrice",
        "UnitPriceDiscount",
        "LineTotal",
    F.to_date("ModifiedDate").alias("DetailModifiedDate")
    )
  )



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_join = (
    df_sales_details
.join(df_sales_orders, "SalesOrderID", how= "inner")
)

df_join = (
    df_join
    .withColumn("ModifiedDate", F.greatest(
        F.col("OrderModifiedDate"),  
        F.col("DetailModifiedDate"))
       
    )
     .withColumn("UpdatedDate", F.current_date())
)

df_join = (
    df_join
    .drop(
        "OrderModifiedDate",
        "DetailModifiedDate")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Particionamento
window = Window.partitionBy("SalesOrderID")

# Criação do Dataframe Final
df_final = df_join.withColumn("OrderQtyTotal", F.sum("OrderQty").over(window))

df_final = (
    df_final
    .select(
        "SalesOrderID",
        "SalesOrderDetailID",
        "ProductID",
        "CustomerID",
        "ShipToAddressID",
        "BillToAddressID",
        # Datas
        "OrderDate",
        "DueDate",
        "ShipDate",
        # Dimenssões
        "SalesOrderNumber",
        "PurchaseOrderNumber",
        "AccountNumber",
        # Valores
        "OrderQty",
        "UnitPrice",
        "UnitPriceDiscount",
        "LineTotal",
        "SubTotal",
        "TaxAmt",
        "Freight",
        "TotalDue",
        # Valores Rateados
        (F.col("SubTotal")* F.col("OrderQty")/ F.col("OrderQtyTotal")). alias("SubTotalRatio"),
        (F.col("TaxAmt")* F.col("OrderQty")/ F.col ("OrderQtyTotal")). alias("TaxAmtRatio"),
        (F.col("Freight")* F.col("OrderQty")/ F.col("OrderQtyTotal")).alias("FreightRatio"),
        (F.col("TotalDue")* F.col("OrderQty")/ F.col("OrderQtyTotal")).alias("TotalDueRatio"),
        # Controle
        "ModifiedDate",
        "UpdatedDate"

        
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_final)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ==============================================================================
# GRAVAÇÃO E TRATAMENTO DA TABELA (VERSÃO ULTRA-BLINDADA CONTRA CACHE)
# ==============================================================================

# Se o pipeline mandar "ws_feature_renan.lh_silver.Sales", isolamos apenas "Sales"
nome_tabela_real = target_table.split(".")[-1]

# Forçamos o Spark a usar o catálogo do Lakehouse atual na raiz de Tables
tabela_nome = f"default.{nome_tabela_real}"

print(f"Nome processado pelo script: {tabela_nome}")

# Verifica se a tabela já existe registrada no catálogo do Lakehouse
tabela_existe = spark.catalog.tableExists(tabela_nome)

if not tabela_existe:
    print(f"Criando tabela identificada no Lakehouse: {tabela_nome}...")
    df_final.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(tabela_nome)  # Registra oficialmente na raiz de 'Tables'

else:
    print(f"Fazendo MERGE na tabela identificada {tabela_nome}...")

    # Instancia a tabela Delta usando o nome mapeado no catálogo
    delta_table = DeltaTable.forName(spark, tabela_nome)

    delta_table.alias("t").merge(
        df_final.alias("s"),
        f"t.{target_key} = s.{target_key}"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
