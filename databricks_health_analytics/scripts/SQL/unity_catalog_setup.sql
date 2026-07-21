-- ==============================================================================
-- DATABASE COMPUTE: Databricks Unity Catalog Setup
-- DESCRIPTION: Sets up structural namespaces and RBAC policy permissions
-- TARGET: Databricks SQL Warehouse or Notebook
-- ==============================================================================

-- 1. Construct the Master Data Container Catalog
CREATE CATALOG IF NOT EXISTS db_hospital_catalog;
USE CATALOG db_hospital_catalog;

-- 2. Construct Medallion Logical Schemas
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- 3. Construct the Atomic Managed Volume Asset inside Staging Silver Schema
CREATE VOLUME IF NOT EXISTS db_hospital_catalog.silver.transformed_records;

-- 4. Grant Granular Access Rights to ADF System Identity (azadfrtlsls / adf-hospital-orchestrator)
-- Granting Catalog Use
GRANT USE CATALOG ON CATALOG `db_hospital_catalog` TO `azadfrtlsls`;
GRANT USE CATALOG ON CATALOG `db_hospital_catalog` TO `adf-hospital-orchestrator`;

-- Granting Schema Use for Staging (Silver)
GRANT USE SCHEMA ON SCHEMA `db_hospital_catalog`.`silver` TO `azadfrtlsls`;
GRANT USE SCHEMA ON SCHEMA `db_hospital_catalog`.`silver` TO `adf-hospital-orchestrator`;

-- Granting Read & Write Access on Staging Managed Volume
GRANT READ VOLUME, WRITE VOLUME ON VOLUME `db_hospital_catalog`.`silver`.`transformed_records` TO `adf-hospital-orchestrator`;

-- Granting Schema Use and Table DDL/DML Actions on Production (Gold)
GRANT USE SCHEMA ON SCHEMA `db_hospital_catalog`.`gold` TO `azadfrtlsls`;
GRANT SELECT, MODIFY, CREATE TABLE ON SCHEMA `db_hospital_catalog`.`gold` TO `azadfrtlsls`;
