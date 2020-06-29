-- filePeople = "/mnt/training/dataframes/people-10m.parquet"
-- fileNames = "dbfs:/mnt/training/ssn/names.parquet"

CREATE DATABASE IF NOT EXISTS Databricks;
USE Databricks;

-- People10M Table
-- 
CREATE TABLE IF NOT EXISTS People10M
USING parquet
LOCATION '/mnt/training/dataframes/people-10m.parquet'
;


-- SSANames Table
--
CREATE TABLE IF NOT EXISTS SSANames
USING parquet
LOCATION '/mnt/training/ssn/names.parquet'
;
