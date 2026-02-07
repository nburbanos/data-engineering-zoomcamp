# Q1
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-course-485501.nytaxi.external_yellow_tripdata_2024`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://kestra-zoomcamp-natalia-demo/yellow_tripdata_2024-*.parquet']
);

SELECT COUNT(*) FROM `dtc-de-course-485501.nytaxi.external_yellow_tripdata_2024`


# Q2

# Creating materialized table from external table
CREATE TABLE `dtc-de-course-485501.nytaxi.yellow_tripdata_2024` AS
SELECT * FROM `dtc-de-course-485501.nytaxi.external_yellow_tripdata_2024`;

# COUNT Distinct # of PULocationIDs

SELECT COUNT(DISTINCT PULocationID) AS uniquePULocationIDs FROM `dtc-de-course-485501.nytaxi.external_yellow_tripdata_2024`; #0MB

SELECT COUNT(DISTINCT PULocationID) AS uniquePULocationIDs FROM `dtc-de-course-485501.nytaxi.yellow_tripdata_2024`; #155.12 MB

#Q3

SELECT PULocationID FROM `dtc-de-course-485501.nytaxi.yellow_tripdata_2024`; #155
SELECT PULocationID, DOLocationID FROM `dtc-de-course-485501.nytaxi.yellow_tripdata_2024`; #310

#Q4

SELECT COUNT(*) FROM `dtc-de-course-485501.nytaxi.yellow_tripdata_2024` WHERE fare_amount = 0;

#Q5

CREATE OR REPLACE TABLE dtc-de-course-485501.nytaxi.yellow_tripdata_2024_partitioned_clustered
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM dtc-de-course-485501.nytaxi.external_yellow_tripdata_2024;

#Q6

SELECT DISTINCT VendorID FROM  `dtc-de-course-485501.nytaxi.yellow_tripdata_2024`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
#Estimated: 310.24 MB

SELECT DISTINCT VendorID FROM  `dtc-de-course-485501.nytaxi.yellow_tripdata_2024_partitioned_clustered`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
#Estimated: 26.84 MB
