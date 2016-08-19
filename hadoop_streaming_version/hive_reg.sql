CREATE TABLE trip_data(
     medallion STRING, hack_license STRING,
     vender_id STRING, rate_code INT,
     store_and_fwd_flag STRING,
     pickup_date_time STRING, dropoff_date_time STRING,
     passenger_count INT, trip_time_in_secs INT,
     trip_distance FLOAT, pickup_longitude DOUBLE,
     pickup_latitude DOUBLE, dropoff_longitude DOUBLE,
     dropoff_latitude DOUBLE)
     ROW FORMAT DELIMITED
     FIELDS TERMINATED BY ','
     STORED AS TEXTFILE
     tblproperties ('skip.header.line.count'='1');
LOAD DATA INPATH '/ArsMing276/NYU_TAXI/trip_data*.txt' OVERWRITE INTO TABLE trip_data;

CREATE TABLE trip_fare(
     medallion STRING, hack_license STRING,
     vender_id STRING, 
     pickup_date_time STRING, payment_type STRING,
     fare_amount FLOAT, surcharge FLOAT,
     mta_tax FLOAT, tip_amount FLOAT,
     tolls_amount FLOAT, total_amount FLOAT)
     ROW FORMAT DELIMITED
     FIELDS TERMINATED BY ','
     STORED AS TEXTFILE
     tblproperties ('skip.header.line.count'='1');
LOAD DATA INPATH '/ArsMing276/NYU_TAXI/trip_fare*.txt' OVERWRITE INTO TABLE trip_fare;


INSERT OVERWRITE DIRECTORY '/ArsMing276/NYU_TAXI_OUT' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
SELECT trip_data.medallion, trip_time_in_secs, tolls_amount, total_amount
FROM trip_data INNER JOIN trip_fare
ON (trip_data.medallion = trip_fare.medallion)
   AND (trip_data.hack_license = trip_fare.hack_license)
   AND (trip_data.vender_id = trip_fare.vender_id)
   AND (trip_data.pickup_date_time = trip_fare.pickup_date_time);






