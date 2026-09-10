# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "b0b63eeb-2916-8070-4294-4a560f956fd2",
# META       "default_lakehouse_name": "LH_Sales",
# META       "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000000",
# META       "known_lakehouses": [
# META         {
# META           "id": "b0b63eeb-2916-8070-4294-4a560f956fd2",
# META           "workspace_id": "00000000-0000-0000-0000-000000000000"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

data = [
    (1, "2026-09-01", "North", "Laptop",   2, 55000.0),
    (2, "2026-09-01", "South", "Monitor",  5, 12000.0),
    (3, "2026-09-02", "West",  "Keyboard", 10, 1500.0),
    (4, "2026-09-03", "East",  "Laptop",   1, 55000.0),
    (5, "2026-09-03", "North", "Mouse",    20,  800.0),
]
cols = ["order_id", "order_date", "region", "product", "quantity", "unit_price"]

df = (spark.createDataFrame(data, cols)
        .withColumn("order_date", F.to_date("order_date"))
        .withColumn("revenue", F.col("quantity") * F.col("unit_price")))

df.write.mode("overwrite").format("delta").saveAsTable("sales")
display(spark.sql("SELECT region, SUM(revenue) AS revenue FROM sales GROUP BY region"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

(spark.table("sales")
    .groupBy("region")
    .agg(F.sum("revenue").alias("total_revenue"),
         F.sum("quantity").alias("total_quantity"))
    .write.mode("overwrite").format("delta").saveAsTable("sales_by_region"))

display(spark.table("sales_by_region"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

(spark.table("sales")
    .groupBy("region")
    .agg(F.sum("revenue").alias("total_revenue"),
         F.sum("quantity").alias("total_quantity"))
    .write.mode("overwrite").format("delta").saveAsTable("sales_by_region"))

display(spark.table("sales_by_region"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
