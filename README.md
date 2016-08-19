# NYU_Taxi_BigData

This is the second project focusing on big data. In this project, I tried several different approaches and tools to finish the same tasks on a big data set, and compared their performance. 

In detail, Implemented two tasks (Calculated Deciles and Fitted Linear Regression) with NYU Taxi Trips Data. The data is more
than 56G, it's website is http://www.andresmh.com/nyctaxitrips/

Several programming tools were tried to process this ‘Big Data’, Hadoop, Hive and Pig were deployed on two machines, Spark was deployed in standalone mode. These approaches include: 

1. In R with parallel computation package – snow, also used Linux commands to help clean and load the data and adopted
Bags of Little Bootstrap method to compute confidence interval of regression coefficients.
2. In lower level language C++ to do the same tasks as in R.
3. In Hadoop, wrote MapReduce code in Python. Used Hive to merge different files and prepare input for MapReduce jobs.
4. In Spark, wrote spark job in Python to calculate deciles and used MLlib to fit the linear regession, which outperformed other approaches.
5. In Pig, wrote Pig Latin Script to calculate decile and fit the linear regression.
