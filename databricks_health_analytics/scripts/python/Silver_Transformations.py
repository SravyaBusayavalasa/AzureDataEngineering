# Databricks notebook source
# DBTITLE 1,Cell 1

# ==============================================================================
# CELL 1: INVOKE CONNECTIVITY ORCHESTRATION
# ==============================================================================
# ==========================================
# STEP 1: CONFIGURE ENVIRONMENT VARIABLES
# ==========================================
STORAGE_ACCOUNT = "azrtlslsgen2" 
CONTAINER_NAME  = "bronze"                 
FILE_PATH       = "Project2/Encounters/2026/06/19/18/44/Encounters.csv" 

# Credentials from your Service Principal setup
CLIENT_ID = "89b4a0f8-0d6b-41b4-9c65-70985dbcb777"
TENANT_ID = "4cbfd5a1-6841-4b4a-8a34-51192afa1a64"

# ==========================================
# STEP 2: FETCH SECRET FROM KEY VAULT SCOPE
# ==========================================
# Databricks safely reaches into Key Vault using your secret scope link
CLIENT_SECRET = dbutils.secrets.get(scope="hospital-scope", key="secret")

# ==========================================
# STEP 3: INJECT SPARK SESSION CONFIGURATIONS
# ==========================================
# This tells the active Spark cluster engine how to log into your specific storage account
spark.conf.set(f"fs.azure.account.auth.type.{STORAGE_ACCOUNT}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{STORAGE_ACCOUNT}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{STORAGE_ACCOUNT}.dfs.core.windows.net", CLIENT_ID)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{STORAGE_ACCOUNT}.dfs.core.windows.net", CLIENT_SECRET)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{STORAGE_ACCOUNT}.dfs.core.windows.net", f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/token")

# ==============================================================================
# CELL 2: GLOBAL ENVIRONMENT PATHS & UTILITIES
# ==============================================================================
from pyspark.sql.functions import col, current_timestamp, to_timestamp, to_date, row_number
from pyspark.sql.window import Window

# 1. Define the widget parameter (this safely defaults to an empty string if run manually)
dbutils.widgets.text("folder_path_param", "")

# 2. Extract the string value passed dynamically from ADF
# Example incoming value from ADF: "2026/06/19/18/40/"
dynamic_date_path = dbutils.widgets.get("folder_path_param")
#dynamic_date_path = "2026/06/19/18/49"
print(dynamic_date_path)
STORAGE_ACCOUNT = "azrtlslsgen2" 

# Base landing URL where your source data lands
base_raw_path = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# Target Unity Catalog Managed Volume file directory path
base_volume_path = "/Volumes/db_hospital_catalog/silver/transformed_records"

def remove_duplicates(df, primary_key_cols, order_by_col):
    """Identifies and eliminates duplicates"""
    window_spec = Window.partitionBy(primary_key_cols).orderBy(col(order_by_col).desc())
    return df.withColumn("row_num", row_number().over(window_spec)) \
             .filter(col("row_num") == 1) \
             .drop("row_num")

# ==============================================================================
# CELL 3: DATA PIPELINE PROCESSING CORE
# ==============================================================================

# 1. DEPARTMENTS TABLE
# ------------------------------------------------------------------------------
print("Processing Departments...")
dept_dynamic_path = f"{base_raw_path}/Project2/Departments/{dynamic_date_path}/Departments.csv"

file_exists = False
has_content = False

try:
    # 1. Check if the file exists in storage via DBUtils
    file_info = dbutils.fs.ls(dept_dynamic_path)
    if len(file_info) > 0:
        file_exists = True
        
        # 2. Check if the physical file size is greater than 0 bytes
        if file_info[0].size > 0:
            has_content = True
            
except Exception as e:
    print(f"Path Discovery Skipped: File path does not exist: {dept_dynamic_path}")

if file_exists and has_content:
    
    # Load the raw CSV data safely
    dept_raw = spark.read.csv(dept_dynamic_path, header=True, inferSchema=True)
    
    # 3. Final Check: Ensure it isn't just a header row with 0 actual data rows
    if not dept_raw.isEmpty():
        print(f"Processing data for: {dept_dynamic_path}")
        
        dept_clean = dept_raw \
            .filter(col("Department ID").isNotNull()) \
            .select(
                col("Department ID").cast("int").alias("department_id"),
                col("Department Name").alias("department_name"),
                col("Hospital ID").cast("int").alias("hospital_id"),
                col("Specialty Description").alias("specialty_description")
            )
            
        dept_final = remove_duplicates(dept_clean, ["department_id", "hospital_id"], "department_id") \
            .withColumn("silver_processed_at", current_timestamp())
            
        # Saved as Parquet into your Unity Catalog Managed Volume directory
        dept_final.write \
            .format("parquet") \
            .mode("overwrite") \
            .save(f"{base_volume_path}/departments")
            
        print("Silver departments directory successfully appended.")
        
    else:
        print(f"File at '{dept_dynamic_path}' contains headers but has 0 data rows. Skipping write.")
else:
    print(f"File at '{dept_dynamic_path}' is completely empty (0 bytes) or missing. Skipping step safely.")


# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview the encounters data directly from the Volume path
# MAGIC SELECT * FROM parquet.`/Volumes/db_hospital_catalog/silver/transformed_records/departments` LIMIT 10;

# COMMAND ----------

# 2. ENCOUNTERS TABLE
# ------------------------------------------------------------------------------
print("Processing Encounters...")
from pyspark.sql.functions import to_timestamp, col, regexp_replace,substring,expr
# Define path
enct_dynamic_path = f"{base_raw_path}/Project2/Encounters/{dynamic_date_path}/Encounters.csv"

file_exists = False
has_content = False

try:
    # 1. Check if the file exists in storage via DBUtils
    file_info = dbutils.fs.ls(enct_dynamic_path)
    if len(file_info) > 0:
        file_exists = True
        
        # 2. Check if the physical file size is greater than 0 bytes
        if file_info[0].size > 0:
            has_content = True
            
except Exception as e:
    print(f"Path Discovery Skipped: File path does not exist: {enct_dynamic_path}")

# ==============================================================================
# CONDITIONAL PIPELINE EXECUTION
# ==============================================================================
if file_exists and has_content:
    
    # Set legacy parser policy inside the execution block for timestamp transformation safety
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    
    # Load the raw CSV data safely
    enc_raw = spark.read.csv(enct_dynamic_path, header=True, inferSchema=True)
    
    # 3. Final Check: Ensure it isn't just a header row with 0 actual data rows
    if not enc_raw.isEmpty():
        print(f"Processing encounter data for: {enct_dynamic_path}")
        
        # Parse timestamp string values cleanly to datetime objects
        enc_raw =enc_raw \
        .withColumn("admission_datetime", 
                    expr("try_to_timestamp(`Patient Admission Datetime`, 'yyyy-MM-dd HH:mm:ss.SSSSSSS')")) \
        .withColumn(
            "Patient Discharge Datetime", 
            to_timestamp(
                regexp_replace(col("Patient Discharge Datetime"), r"\s+", " "), # Replaces double spaces with a single space
                "MMM d yyyy h:mma"
            )
        )

        # Transformation logic and column mapping
        enc_clean = enc_raw \
            .filter(col("Patient Encounter ID").isNotNull()) \
            .drop("Patient LOS Bucket Sort") \
            .withColumn("discharge_datetime", to_timestamp(col("Patient Discharge Datetime"), "yyyy-MM-dd HH:mm:ss")) \
            .select(
                col("Patient Encounter ID").cast("int").alias("encounter_id"),
                col("Master Patient ID").cast("int").alias("patient_id"),
                col("Attending Provider ID").cast("int").alias("provider_id"),
                col("admission_datetime"),
                col("Patient Discharge Datetime").alias("discharge_datetime"),
                col("Patient LOS").cast("double").alias("length_of_stay_days"),
                col("Patient LOS Bucket").alias("los_bucket"),
                col("Department ID").cast("int").alias("department_id"),
                col("Hospital Account ID").cast("int").alias("hospital_account_id"),
                col("Patient InICU Flag").alias("in_icu_flag"),
                col("Patient Admitted Flag").alias("is_admitted_flag"),
                col("Patient Readmission Flag").alias("is_readmission_flag")
            )
            
        # Deduplicate over structural key identifier
        enc_final = remove_duplicates(enc_clean, ["encounter_id"], "admission_datetime") \
            .withColumn("silver_processed_at", current_timestamp())
            
        # Saved as Parquet into your Unity Catalog Managed Volume directory
        enc_final.write \
            .format("parquet") \
            .mode("overwrite") \
            .save(f"{base_volume_path}/encounters")
            
        print("Silver encounters directory successfully appended.")
        
    else:
        print(f"File at '{enct_dynamic_path}' contains headers but has 0 data rows. Skipping write.")
else:
    print(f"File at '{enct_dynamic_path}' is completely empty (0 bytes) or missing. Skipping step safely.")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Preview the encounters data directly from the Volume path
# MAGIC SELECT * FROM parquet.`/Volumes/db_hospital_catalog/silver/transformed_records/encounters` where length_of_stay_days > 1 LIMIT 10;

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, coalesce, lit, split, expr

# ==============================================================================
# 3. HOSPITALS TABLE
# ==============================================================================
print("Processing Hospitals...")
hosp_dynamic_path = f"{base_raw_path}/Project2/Hospitals/{dynamic_date_path}/Hospitals.csv"

hosp_exists = False
hosp_has_content = False

try:
    hosp_info = dbutils.fs.ls(hosp_dynamic_path)
    if len(hosp_info) > 0 and hosp_info[0].size > 0:
        hosp_exists = True
        hosp_has_content = True
except Exception as e:
    print(f"Path Discovery Skipped: Hospitals file path does not exist: {hosp_dynamic_path}")

if hosp_exists and hosp_has_content:
    hosp_raw = spark.read.csv(hosp_dynamic_path, header=True, inferSchema=True)
    
    if not hosp_raw.isEmpty():
        hosp_clean = hosp_raw \
            .filter(col("Hospital ID").isNotNull()) \
            .drop("Hospital Address Number", "Hospital Address Street") \
            .select(
                col("Hospital ID").cast("int").alias("hospital_id"),
                col("Hospital Name").alias("hospital_name"),
                col("Hospital Address Full").alias("address"),
                col("Hospital City").alias("city"),
                col("Hospital Zip Code").alias("zip_code"),
                col("Hospital Latitude").cast("double").alias("latitude"),
                col("Hospital Longitude").cast("double").alias("longitude")
            )
        hosp_final = remove_duplicates(hosp_clean, ["hospital_id"], "hospital_id") \
            .withColumn("silver_processed_at", current_timestamp())

        hosp_final.write.format("parquet").mode("overwrite").save(f"{base_volume_path}/hospitals")
        print("Silver hospitals table successfully updated.")
    else:
        print(f"Warning: Hospitals file at '{hosp_dynamic_path}' contains headers but has 0 data rows.")
else:
    print(f"Halted: Hospitals file at '{hosp_dynamic_path}' is completely empty or missing.")


# ==============================================================================
# 4. PATIENTS TABLE
# ==============================================================================
print("Processing Patients...")
pat_dynamic_path = f"{base_raw_path}/Project2/Patients/{dynamic_date_path}/Patients.csv"

pat_exists = False
pat_has_content = False

try:
    pat_info = dbutils.fs.ls(pat_dynamic_path)
    if len(pat_info) > 0 and pat_info[0].size > 0:
        pat_exists = True
        pat_has_content = True
except Exception as e:
    print(f"Path Discovery Skipped: Patients file path does not exist: {pat_dynamic_path}")

if pat_exists and pat_has_content:
    pat_raw = spark.read.csv(pat_dynamic_path, header=True, inferSchema=True)
    
    if not pat_raw.isEmpty():
        pat_clean = pat_raw \
            .filter(col("Master Patient ID").isNotNull()) \
            .withColumn("date_of_birth", coalesce(
                expr("try_to_date(split(trim(`Patient DOB`), ' ')[0], 'M/d/yyyy')"),
                lit("1900-01-01").cast("date")
            )) \
            .select(
                col("Master Patient ID").cast("int").alias("patient_id"),
                col("Patient Name").alias("patient_name"),
                col("date_of_birth"),
                col("Patient Gender").alias("gender"),
                col("Patient Marital Status").alias("marital_status"),
                col("Patient LACE Score").try_cast("int").alias("lace_score"),
                col("Patient City").alias("city"),
                col("Patient State").alias("state"),
                col("Patient Ethnicity").alias("ethnicity")
            )
        pat_final = remove_duplicates(pat_clean, ["patient_id"], "patient_id") \
            .withColumn("silver_processed_at", current_timestamp())

        pat_final.write.format("parquet").mode("overwrite").save(f"{base_volume_path}/patients")
        print("Silver patients table successfully updated.")
    else:
        print(f"Warning: Patients file at '{pat_dynamic_path}' contains headers but has 0 data rows.")
else:
    print(f"Halted: Patients file at '{pat_dynamic_path}' is completely empty or missing.")


# ==============================================================================
# 5. PHYSICIANS TABLE
# ==============================================================================
print("Processing Physicians...")
phy_dynamic_path = f"{base_raw_path}/Project2/Physicians/{dynamic_date_path}/Physicians.csv"

phy_exists = False
phy_has_content = False

try:
    phy_info = dbutils.fs.ls(phy_dynamic_path)
    if len(phy_info) > 0 and phy_info[0].size > 0:
        phy_exists = True
        phy_has_content = True
except Exception as e:
    print(f"Path Discovery Skipped: Physicians file path does not exist: {phy_dynamic_path}")

if phy_exists and phy_has_content:
    phys_raw = spark.read.csv(phy_dynamic_path, header=True, inferSchema=True)
    
    if not phys_raw.isEmpty():
        phys_clean = phys_raw \
            .filter(col("Provider ID").isNotNull()) \
            .select(
                col("Provider ID").cast("int").alias("provider_id"),
                col("Provider First Name").alias("first_name"),
                col("Provider Last Name").alias("last_name"),
                col("Provider Full Name").alias("full_name")
            )
        phys_final = remove_duplicates(phys_clean, ["provider_id"], "provider_id") \
            .withColumn("silver_processed_at", current_timestamp())

        phys_final.write.format("parquet").mode("overwrite").save(f"{base_volume_path}/physicians")
        print("Silver physicians table successfully updated.")
    else:
        print(f"Warning: Physicians file at '{phy_dynamic_path}' contains headers but has 0 data rows.")
else:
    print(f"Halted: Physicians file at '{phy_dynamic_path}' is completely empty or missing.")

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC -- Preview the encounters data directly from the Volume path
# MAGIC SELECT * FROM parquet.`/Volumes/db_hospital_catalog/silver/transformed_records/patients` limit 10 ;
# MAGIC --SELECT * FROM parquet.`/Volumes/db_hospital_catalog/silver/transformed_records/physicians` limit 10 ;
# MAGIC --SELECT * FROM parquet.`/Volumes/db_hospital_catalog/silver/transformed_records/hospitals` limit 10 ;