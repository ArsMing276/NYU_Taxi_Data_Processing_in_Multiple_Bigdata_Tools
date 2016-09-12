/*we first used shell to delete header row since pig is not convenient to do so. The command is -- 
sed '1d' file.txt > tmpfile; mv tmpfile file.txt*/
trip_data = load '/home/stanley/Downloads/NYU_TAXI/trip_data' using PigStorage(',') as 
(medallion:chararray,hack_license:chararray,vendor_id:chararray,rate_code:int,store_and_fwd_flag:
bytearray,pickup_datetime:chararray,dropoff_datetime:chararray, passenger_count:int,trip_time_in_secs:int,
trip_distance:float,pickup_longitude:double,pickup_latitude:double,dropoff_longitude:double,dropoff_latitude:double);

trip_fare = load '/home/stanley/Downloads/NYU_TAXI/trip_fare' using PigStorage(',') as (medallion:chararray,
hack_license:chararray,vendor_id:chararray, pickup_datetime:chararray, payment_type:chararray, fare_amount:float, 
surcharge:float, mta_tax:float, tip_amount:float, tolls_amount:float, total_amount:float);

--select only useful columns at first to reduce workload of the following steps.
trip_data_pred = foreach trip_data generate medallion, hack_license, vendor_id, trip_time_in_secs as X;
trip_fare_pred = foreach trip_fare generate medallion, hack_license, vendor_id, (total_amount - tolls_amount) as Y;

--we joined the two data together with three columns for robustness although we are sure they match each other from previous approaches. 
trip_joined = join trip_data_pred by (medallion, hack_license, vendor_id), trip_fare_pred by (medallion, hack_license, vendor_id) parallel 6;
trip_joined_stats = foreach trip_joined generate (X*Y) as XY, (X*X) as Xsq, X, Y, 1 as N;
trip_grpd = group trip_joined_stats all;
trip_final_stats = foreach trip_grpd generate SUM(trip_joined_stats.XY) as XY, SUM(trip_joined_stats.Xsq) as Xsq, 
SUM(trip_joined_stats.X) as X, SUM(trip_joined_stats.Y) as Y, SUM(trip_joined_stats.N) as N;
dump trip_final_stats;
--next, we can use the regression formula to calculate slope and intercept.
