from pyspark.sql import functions as F

# 1. Define paths using the primary abfss endpoint style for Synapse
source_path = "abfss://nyc-taxi-data@adlsgen2snps.dfs.core.windows.net/yellow_tripdata_2026-*.parquet"

print(f"Reading raw ingestion data from: {source_path}")

# 2. Read the raw Parquet data
df_raw = spark.read.format("parquet").load(source_path)

# 3. Engineer Partition Columns (Year and Month)
df_partitioned = df_raw \
    .withColumn("pickup_year", F.year(F.col("tpep_pickup_datetime"))) \
    .withColumn("pickup_month", F.month(F.col("tpep_pickup_datetime")))

df_cleaned_partitions = df_partitioned.filter(F.col("pickup_year") == 2026)

# 4. Save as a Managed Delta Table in the Synapse Lakehouse Spark Catalog

target_table = "default.nyc_taxi_yellow"
print(f"Dropping table if it exists: {target_table}")
spark.sql(f"DROP TABLE IF EXISTS {target_table}")

print(f"Writing data to Synapse Delta table: {target_table}")

df_cleaned_partitions.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("pickup_year", "pickup_month") \
    .saveAsTable(target_table)

print("Synapse Spark partition write executed successfully.")