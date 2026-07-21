
--1.create external file format
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'DeltaLakeFormat')
BEGIN
    CREATE EXTERNAL FILE FORMAT [DeltaLakeFormat]
    WITH (  
        FORMAT_TYPE = DELTA  
    );
END;
GO

--2.create scoped credential
CREATE DATABASE SCOPED CREDENTIAL [SynapseMSICredential]
WITH IDENTITY = 'Managed Identity';
GO


-- 3. create external data source attached to the Managed Identity credential
CREATE EXTERNAL DATA SOURCE [restapi-data_adlsgen2snps_dfs_core_windows_net]
WITH (
    LOCATION = 'abfss://restapi-data@adlsgen2snps.dfs.core.windows.net',
    CREDENTIAL = [SynapseMSICredential]
);
GO

--4.create external table pointing to this data source
CREATE EXTERNAL TABLE [dbo].[nyc_taxi_2026] (
	[VendorID] int,
	[tpep_pickup_datetime] datetime2(7),
	[tpep_dropoff_datetime] datetime2(7),
	[passenger_count] bigint,
	[trip_distance] float,
	[RatecodeID] bigint,
	[store_and_fwd_flag] nvarchar(4000),
	[PULocationID] int,
	[DOLocationID] int,
	[payment_type] bigint,
	[fare_amount] float,
	[extra] float,
	[mta_tax] float,
	[tip_amount] float,
	[tolls_amount] float,
	[improvement_surcharge] float,
	[total_amount] float,
	[congestion_surcharge] float,
	[Airport_fee] float,
	[cbd_congestion_fee] float
	)
WITH (
    LOCATION = 'synapse/workspaces/azsynapsews/warehouse/nyc_taxi_yellow', 
    DATA_SOURCE = [restapi-data_adlsgen2snps_dfs_core_windows_net],
    FILE_FORMAT = [DeltaLakeFormat]
);
GO



-- 5.create view featuring the last 2 months of 2026 yellow taxi data
CREATE OR ALTER VIEW dbo.vw_nyc_taxi_last_two_months AS
WITH MaxDateCTE AS (
    SELECT DATEADD(month, -2, MAX([tpep_pickup_datetime])) AS [CutoffDate]
    FROM OPENROWSET(
        BULK 'synapse/workspaces/azsynapsews/warehouse/nyc_taxi_yellow',
        DATA_SOURCE = 'restapi-data_adlsgen2snps_dfs_core_windows_net',
        FORMAT = 'DELTA'
    ) AS [sub]
)
SELECT 
    *,
    MONTH([taxi].[tpep_pickup_datetime]) AS [Pickup_Month_Number],
    DATENAME(month, [taxi].[tpep_pickup_datetime]) AS [Pickup_Month_Name]
FROM OPENROWSET(
    BULK 'synapse/workspaces/azsynapsews/warehouse/nyc_taxi_yellow',
    DATA_SOURCE = 'restapi-data_adlsgen2snps_dfs_core_windows_net',
    FORMAT = 'DELTA'
) AS [taxi]
CROSS JOIN MaxDateCTE
WHERE 
    [taxi].[tpep_pickup_datetime] >= [MaxDateCTE].[CutoffDate];
GO

--select count(*),[Pickup_Month_Number] from vw_nyc_taxi_last_two_months group by [Pickup_Month_Number] 