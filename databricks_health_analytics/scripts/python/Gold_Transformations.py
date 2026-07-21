# Databricks notebook source
from pyspark.sql.functions import col, current_timestamp, datediff, lit, when, year, expr, count, countDistinct
from pyspark.sql.window import Window
from delta.tables import DeltaTable

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Define base paths to Silver Parquet tables/directories
silver_base_path = "/Volumes/db_hospital_catalog/silver/transformed_records"

print("Starting Gold Transformation Stage...")

# ==============================================================================
# 1. ENCOUNTERS TABLE GOLD TRANSFORMATION
# ==============================================================================
print("Processing Encounters...")

# Pull from Silver Parquet files
df_encounters_silver = spark.read.format("parquet").load(f"{silver_base_path}/encounters")

# Derive additional clinical and financial indicators:
# - actual_stay_days: Derived mathematically from admission and discharge timestamps
# - total_charge: Estimated billable revenue ($1,200/day base)
# - operational_cost: Fixed hospital cost structure
# - gold_processed_at: Audit lineage timestamp
df_encounters_gold = df_encounters_silver \
    .withColumn("actual_stay_days", datediff(col("discharge_datetime"), col("admission_datetime"))) \
    .withColumn("actual_stay_days", when(col("actual_stay_days") == 0, 1).otherwise(col("actual_stay_days"))) \
    .withColumn("total_charge", col("actual_stay_days") * 1200.0) \
    .withColumn("operational_cost", col("actual_stay_days") * 750.0 + 500.0) \
    .withColumn("gold_processed_at", current_timestamp())

# Write to Gold Stage as a Managed Table in APPEND Mode
df_encounters_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .saveAsTable("db_hospital_catalog.gold.encounters_summary")


# ==============================================================================
# 2. PATIENTS TABLE GOLD TRANSFORMATION
# ==============================================================================
print("Processing Patients...")

# Pull from Silver Parquet files
df_patients_silver = spark.read.format("parquet").load(f"{silver_base_path}/patients")

# Derive demographic analytics indicators:
# - current_age: Age computed from date of birth (safely handles the 1900-01-01 default)
# - age_group: Categorized bucket for hospital risk stratification
df_patients_gold = df_patients_silver \
    .withColumn("current_age", when(col("date_of_birth") == "1900-01-01", lit(None))
                               .otherwise(year(current_timestamp()) - year(col("date_of_birth")))) \
    .withColumn("age_group", when(col("current_age") < 18, "Pediatric")
                             .when((col("current_age") >= 18) & (col("current_age") < 65), "Adult")
                             .when(col("current_age") >= 65, "Geriatric")
                             .otherwise("Unknown")) \
    .withColumn("gold_processed_at", current_timestamp())


target_patients = "db_hospital_catalog.gold.patients_demographics"

# Initialize table on first run, otherwise execute SCD 1 Merge
if not spark.catalog.tableExists(target_patients):
    df_patients_gold.write.format("delta").mode("ignore").saveAsTable(target_patients)

DeltaTable.forName(spark, target_patients).alias("target") \
    .merge(df_patients_gold.alias("source"), "target.patient_id = source.patient_id") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
# ==============================================================================
# 3. DEPARTMENTS METRICS TRANSFORMATION
# ==============================================================================
print("Processing Operational Metadata...")

# Pull from Silver Parquet files
df_departments_silver = spark.read.format("parquet").load(f"{silver_base_path}/departments")

# Derive basic business tracking column
df_departments_gold = df_departments_silver \
    .withColumn("is_active_clinical_unit", lit(True)) \
    .withColumn("gold_processed_at", current_timestamp())

target_depts = "db_hospital_catalog.gold.departments_directory"

if not spark.catalog.tableExists(target_depts):
    df_departments_gold.write.format("delta").mode("ignore").saveAsTable(target_depts)

DeltaTable.forName(spark, target_depts).alias("target") \
    .merge(df_departments_gold.alias("source"), "target.department_id = source.department_id") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# ==============================================================================
# 4. HOSPITALS METRICS TRANSFORMATION
# ==============================================================================
# Pull from Silver Parquet files
df_hospitals_silver = spark.read.format("parquet").load(f"{silver_base_path}/hospitals")
zipcode_window = Window.partitionBy("zip_code")
# Derive hospitals count based on zipcode
df_hospitals_gold = df_hospitals_silver \
    .withColumn("zipcode_hospitals_total", count("*").over(zipcode_window)) \
    .withColumn("gold_processed_at", current_timestamp())

target_hospitals = "db_hospital_catalog.gold.hospitals_directory"

if not spark.catalog.tableExists(target_hospitals):
    df_hospitals_gold.write.format("delta").mode("ignore").saveAsTable(target_hospitals)

DeltaTable.forName(spark, target_hospitals).alias("target") \
    .merge(df_hospitals_gold.alias("source"), "target.hospital_id = source.hospital_id") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

# ==============================================================================
# 5. PHYSICIANS METRICS TRANSFORMATION
# ==============================================================================
# Pull from Silver Parquet files
df_physicians_silver = spark.read.format("parquet").load(f"{silver_base_path}/physicians")
provider_window = Window.partitionBy(df_physicians_silver["provider_id"])
# Derive provider encounters count
df_physicians_gold = df_physicians_silver.join(df_encounters_silver, on=df_encounters_silver["provider_id"] == df_physicians_silver["provider_id"], 
    how="left") \
    .withColumn("provider_encounters_total", count('encounter_id').over(provider_window)) \
    .withColumn("gold_processed_at", current_timestamp())

df_physicians_gold = df_physicians_gold.select(df_physicians_silver["provider_id"], "full_name", "provider_encounters_total","gold_processed_at").distinct()

target_physicians = "db_hospital_catalog.gold.physicians_directory"

if not spark.catalog.tableExists(target_physicians):
    df_physicians_gold.write.format("delta").mode("ignore").saveAsTable(target_physicians)

# Execute the final Upsert mapping
DeltaTable.forName(spark, target_physicians).alias("target") \
    .merge(df_physicians_gold.alias("source"), "target.provider_id = source.provider_id") \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()

print("All Gold Managed Tables successfully computed !")