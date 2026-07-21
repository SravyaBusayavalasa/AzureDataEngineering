-- ==============================================================================
-- DATABASE: hospital
-- DESCRIPTION: Watermark Infrastructure for Incremental ETL Extraction Pipeline
-- TARGET: Source SQL Server Database
-- ==============================================================================

-- 1. Create the Watermark Checkpoint Table
CREATE TABLE [dbo].[watermark_table](
    [table_id] [int] IDENTITY(1,1) NOT NULL,
    [source_schema] [varchar](100) NOT NULL,
    [source_table] [varchar](100) NOT NULL,
    [folder_name] [varchar](100) NULL,
    [watermark_column] [varchar](100) NULL,
    [last_load_value] [varchar](100) NULL,
    CONSTRAINT [PK_watermark_table] PRIMARY KEY CLUSTERED ([table_id] ASC)
);
GO

-- 2. Seed Initial Checkpoints for Source Tables
INSERT INTO [dbo].[watermark_table] 
    ([source_schema], [source_table], [folder_name], [watermark_column], [last_load_value]) 
VALUES
    ('dbo', 'Hospitals', 'Project2/Hospitals', 'Hospital ID', '9900006'),
    ('dbo', 'Physicians', 'Project2/Physicians', 'Provider ID', '9999'),
    ('dbo', 'Patients', 'Project2/Patients', 'Master Patient ID', '107386'),
    ('dbo', 'Departments', 'Project2/Departments', 'DepartmentKey', '64'),
    ('dbo', 'Encounters', 'Project2/Encounters', 'Patient Admission Datetime', '2021-07-02T14:46:00');
GO

-- 3. Stored Procedure to Update Checkpoint State
CREATE PROCEDURE dbo.usp_update_watermark_table
    @lastloadvalue VARCHAR(100),
    @tablename     VARCHAR(100),
    @schemaname    VARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE hospital.dbo.watermark_table
    SET last_load_value = @lastloadvalue
    WHERE source_table = @tablename
      AND source_schema = @schemaname;
END;
GO
