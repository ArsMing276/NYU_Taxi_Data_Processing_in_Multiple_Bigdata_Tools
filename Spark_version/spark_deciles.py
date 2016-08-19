# -*- coding: utf-8 -*-

## In spark, we will try different approach, we will sort the sequence instead of making frequency table, 
## also, we will read local NYU taxi files instead of files from HDFS
 
from pyspark import SparkConf, SparkContext, StorageLevel

##configuration depends one machine, we could specify when using YARN as master or use standalone machines as a cluster 
conf = SparkConf().setAppName('NYUTAXI')
conf = (conf.set('spark.executor.memory', '7G')
        .set('spark.cores.max', '8')
        .set('spark.memory.fraction', '0.80')
        .set('spark.driver.memory', '13G')
        .set('spark.driver.maxResultSize', '2G'))
sc = SparkContext(conf = conf)

files = sc.textFile('hdfs:///ArsMing276/trip_fare')

## skip the header row
def taxisplit(value):
    words = value.split(',')
    try:
        val = float(words[-1]) - float(words[-2])
        return val
    except ValueError:
        pass

total_less_toll = files.map(taxisplit).persist(storageLevel=StorageLevel.DISK_ONLY)
num = total_less_toll.count() ##count the total rows
total_less_toll_ordered = total_less_toll.takeOrdered(num) ##sort the total less toll sequence

decile_idx = map(lambda x: int(round(x / float(10) * (num - 1))), range(11))
deciles = map(lambda x: total_less_toll_ordered[x], decile_idx)

deciles.saveAsTextFile('hdfs://ArsMing276/trip_fare_result')
