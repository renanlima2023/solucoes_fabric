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

from pyspark.sql import functions as F
from pyspark.sql.window import Window as W
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
window = W.partitionBy("SalesOrderID")

# Criação do Dataframe Final
df_final = df_join.withColumn(
    "OrderQtyTotal",
    F.sum("OrderQty").over(window)

)

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
