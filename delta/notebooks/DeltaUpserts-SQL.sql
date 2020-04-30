-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC 
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://d1ra4hr810e003.cloudfront.net/visual/accountlogo/0FD7ECF9-0605-4D3A-A2D47EAD3CDEA1E2/small-3932F8F6-8EB0-40B4-8C0ABB8C8554C4CB.jpg" alt="Databricks" >
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Databricks Delta Batch Operations - Upsert
-- MAGIC 
-- MAGIC Databricks&reg; Delta allows you to read, write and query data in data lakes in an efficient manner.
-- MAGIC 
-- MAGIC ## In this lesson you:
-- MAGIC * Use Databricks Delta to UPSERT data into tables
-- MAGIC 
-- MAGIC 
-- MAGIC 
-- MAGIC ## Datasets Used
-- MAGIC We will use online retail datasets from
-- MAGIC * `/mnt/training/online_retail` in the demo part and
-- MAGIC * `/mnt/training/structured-streaming/events/` in the exercises

-- COMMAND ----------

set spark.sql.shuffle.partitions=8

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## UPSERT 
-- MAGIC 
-- MAGIC Literally means "UPdate" and "inSERT". It means to atomically either insert a row, or, if the row already exists, UPDATE the row.
-- MAGIC 
-- MAGIC Alter data by changing the values in one of the columns for a specific `CustomerID`.
-- MAGIC 
-- MAGIC Let's load the CSV file `../outdoor-products-mini.csv`.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 1 Create a Delta table and load test dataset
-- MAGIC - **test dataset path:** /mnt/training/online_retail/outdoor-products/outdoor-products-small.csv

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls /mnt/training/online_retail/outdoor-products/

-- COMMAND ----------

-- DBTITLE 1,1.1 Create delta table
DROP TABLE IF EXISTS customer_data_delta;

CREATE TABLE customer_data_delta(
  `InvoiceNo` INT,
  `StockCode` STRING,
  `Description` STRING,
  `Quantity` INT,
  `InvoiceDate` STRING,
  `UnitPrice` DOUBLE,
  `CustomerID` INT,
  `Country` STRING
  )
  USING DELTA;

-- COMMAND ----------

-- DBTITLE 1,1.2 Check test dataset...
select * from csv.`dbfs:/mnt/training/online_retail/outdoor-products/outdoor-products-small.csv` limit 10

-- COMMAND ----------

select count(*) from csv.`dbfs:/mnt/training/online_retail/outdoor-products/outdoor-products-small.csv` limit 10

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ### COPY INTO Command
-- MAGIC Load data from a file location into a Delta table. <br>
-- MAGIC This is a re-triable and idempotent operation—files in the source location that have already been loaded are skipped.<br>
-- MAGIC https://docs.microsoft.com/en-us/azure/databricks/spark/latest/spark-sql/language-manual/copy-into

-- COMMAND ----------

-- DBTITLE 1,1.3 Load test data into delta table
COPY INTO customer_data_delta
FROM (
  SELECT
  InvoiceNo, CAST(StockCode AS STRING) , Description, Quantity, InvoiceDate, CAST(UnitPrice AS DOUBLE), CustomerID, Country
  FROM 'dbfs:/mnt/training/online_retail/outdoor-products/outdoor-products-small.csv' 
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true');

-- COMMAND ----------

select count(*) from customer_data_delta

-- COMMAND ----------

select * from customer_data_delta limit 10

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC ## 2 Read new batch and do UPSERTS
-- MAGIC - **new batch dataset path:** /mnt/training/online_retail/outdoor-products/outdoor-products-mini.csv

-- COMMAND ----------

-- DBTITLE 1,2.1 Create a view pointing to new file
CREATE OR REPLACE TEMPORARY VIEW customer_data_new_batch
USING CSV
OPTIONS(header "true", inferSchema "true", path "dbfs:/mnt/training/online_retail/outdoor-products/outdoor-products-mini.csv")

-- COMMAND ----------

-- DBTITLE 0,Untitled
-- MAGIC %md-sandbox
-- MAGIC #### Few checks to understand the expected results after UPSERT

-- COMMAND ----------

-- DBTITLE 1,Number of records in new batch
select count(*) from customer_data_upsert;

-- COMMAND ----------

-- DBTITLE 1,Number of distinct customers in new batch
select count(distinct customerID) from customer_data_new_batch;

-- COMMAND ----------

-- DBTITLE 1,How many of these customers are already in delta table
select count(*) from customer_data_new_batch  A
INNER JOIN 
(select distinct customerID from customer_data_delta) as B
ON A.customerID=B.customerID 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Upsert the new data into `customer_data_delta`. For matched records update StockCode<br>
-- MAGIC Upsert is done using the `MERGE INTO` syntax

-- COMMAND ----------

-- DBTITLE 1,One more view for new batch convenience
CREATE OR REPLACE TEMPORARY VIEW customer_data_upsert
AS
SELECT
InvoiceNo, CAST(StockCode AS STRING) , Description, Quantity, InvoiceDate, CAST(UnitPrice AS DOUBLE), CustomerID, Country
FROM customer_data_new_batch
;

-- COMMAND ----------

-- DBTITLE 1,Finally Merge Into delta table
-- MAGIC %sql
-- MAGIC MERGE INTO customer_data_delta as BASE
-- MAGIC USING customer_data_upsert as UPSERT
-- MAGIC ON BASE.CustomerID = UPSERT.CustomerID
-- MAGIC WHEN MATCHED THEN
-- MAGIC   UPDATE SET
-- MAGIC     BASE.StockCode = UPSERT.StockCode
-- MAGIC WHEN NOT MATCHED
-- MAGIC   THEN INSERT (InvoiceNo, StockCode, Description, Quantity, InvoiceDate, UnitPrice, CustomerID, Country)
-- MAGIC   VALUES (
-- MAGIC     UPSERT.InvoiceNo,
-- MAGIC     UPSERT.StockCode, 
-- MAGIC     UPSERT.Description, 
-- MAGIC     UPSERT.Quantity, 
-- MAGIC     UPSERT.InvoiceDate, 
-- MAGIC     UPSERT.UnitPrice, 
-- MAGIC     UPSERT.CustomerID, 
-- MAGIC     UPSERT.Country)
-- MAGIC ;

-- COMMAND ----------

-- DBTITLE 1,Merge into delta table - variation
-- MAGIC %sql
-- MAGIC MERGE INTO customer_data_delta as BASE
-- MAGIC USING customer_data_upsert as UPSERT
-- MAGIC ON BASE.CustomerID = UPSERT.CustomerID
-- MAGIC WHEN MATCHED THEN UPDATE SET *
-- MAGIC WHEN NOT MATCHED THEN INSERT * 
-- MAGIC ;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Exercise 1
-- MAGIC Delete CustomerIds 20035, 20051, 20123 **customer_data_delta** table and execute the merge again.<br>
-- MAGIC **Reference**: https://docs.databricks.com/delta/delta-update.html#table-deletes-updates-and-merges

-- COMMAND ----------

-- TODO

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Exercise 2
-- MAGIC 
-- MAGIC Run describe history on customer_data_delta to check operations performed on the table. <br>
-- MAGIC **Reference:** https://docs.databricks.com/delta/delta-utility.html#history

-- COMMAND ----------

-- TODO

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Exercise 3
-- MAGIC 
-- MAGIC Run OPTIMIZE on customer_data_delta table. <br>
-- MAGIC **Reference:** https://docs.microsoft.com/en-us/azure/databricks/spark/latest/spark-sql/language-manual/optimize?toc=https%3A%2F%2Fdocs.microsoft.com%2Fen-us%2Fazure%2Fazure-databricks%2Ftoc.json&bc=https%3A%2F%2Fdocs.microsoft.com%2Fen-us%2Fazure%2Fbread%2Ftoc.json

-- COMMAND ----------

-- TODO

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Exercise 4
-- MAGIC 
-- MAGIC Run OPTIMIZE with ZORDER on customer_data_delta table. <br>
-- MAGIC **Reference:** https://docs.microsoft.com/en-us/azure/databricks/spark/latest/spark-sql/language-manual/optimize?toc=https%3A%2F%2Fdocs.microsoft.com%2Fen-us%2Fazure%2Fazure-databricks%2Ftoc.json&bc=https%3A%2F%2Fdocs.microsoft.com%2Fen-us%2Fazure%2Fbread%2Ftoc.json

-- COMMAND ----------

-- TODO
