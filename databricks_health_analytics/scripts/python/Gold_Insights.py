# Databricks notebook source
from pyspark.sql.functions import col, desc, max, sum, count

# Define your Gold catalog and schema identifier
gold_schema = "db_hospital_catalog.gold"

print("Extracting Healthcare Insights from Gold Layer...\n")

# ==============================================================================
# 1. Patient with the Longest Hospital Stay
# ==============================================================================
# Join encounters summary with demographics to get the patient's name
df_encounters = spark.table(f"{gold_schema}.encounters_summary")
df_patients = spark.table(f"{gold_schema}.patients_demographics")

longest_stay = df_encounters.join(df_patients, "patient_id", "inner") \
    .select("patient_id", "patient_name", "actual_stay_days") \
    .orderBy(desc("actual_stay_days")) \
    .limit(1)

print("Patient with Most Stay Days:")
longest_stay.show(truncate=False)


# ==============================================================================
# 2. Patient Who Spent the Highest (Top Billing Revenue)
# ==============================================================================
# Aggregate multiple stays if a patient was readmitted over time
highest_spender = df_encounters.groupBy("patient_id") \
    .agg(sum("total_charge").alias("cumulative_spend")) \
    .join(df_patients, "patient_id", "inner") \
    .select("patient_id", "patient_name", "cumulative_spend") \
    .orderBy(desc("cumulative_spend")) \
    .limit(1)

print("Patient with Highest Financial Billing:")
highest_spender.show(truncate=False)


# ==============================================================================
# 3. Healthcare Provider (Physician) with the Most Encounters
# ==============================================================================
df_physicians = spark.table(f"{gold_schema}.physicians_directory")

# Since provider_encounters_total is calculated in the gold layer, we can query it directly
top_provider = df_physicians \
    .select("provider_id", "full_name", "provider_encounters_total") \
    .orderBy(desc("provider_encounters_total")) \
    .limit(1)

print("Provider with Most Patient Encounters:")
top_provider.show(truncate=False)


# ==============================================================================
# 4. City with the Highest Density of Hospitals
# ==============================================================================
df_hospitals = spark.table(f"{gold_schema}.hospitals_directory")

# Group by city to find the regional leader
top_city = df_hospitals \
    .groupBy("city") \
    .agg(count("hospital_id").alias("hospital_count")) \
    .orderBy(desc("hospital_count")) \
    .limit(1)

print("City with the Most Hospitals:")
top_city.show(truncate=False)