# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Validar a existencia de tabela delta
def table_exists(path:str) -> bool:
    from pyspark.sql.utils import AnalysisException
    try:
        spark.read.format("delta").load(path)
        return True
      
    except AnalysisException:
        return False
     

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def safe_merge(source, path, key):
    if table_exists(path):
        from delta.tables import DeltaTable
        # Variavies para o merge
        target = DeltaTable.forPath(spark, path)
        colmuns = source.columns
        update_cols= {col_name: f"source.{col_name}" for col_name in colmuns}
        update_condition = "source.ModifiedDate > target.ModifiedDate"
        merge_condition = f"target.{key} = source.{key}"

        # Instrução Merge
        (
            target.alias("target")
            .merge(source=source.alias("source"), condition=merge_condition)
            .whenMatchedUpdate(condition = update_condition, set=update_cols)
            .whenNotMatchedInsert(values=update_cols)
            .execute()
        )
    else:
        source.write.format("delta").mode("overwrite").save(path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
