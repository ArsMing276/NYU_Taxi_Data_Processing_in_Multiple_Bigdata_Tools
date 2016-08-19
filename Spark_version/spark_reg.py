##For the regression problem, we will use MLLIB

from pyspark import SparkContext, SparkConf
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD

conf = SparkConf().setAppName('NYUTAXI')
conf = (conf.set('spark.executor.memory', '7G')
        .set('spark.cores.max', '8')
        .set('spark.memory.fraction', '0.80')
        .set('spark.driver.memory', '13G')
        .set('spark.driver.maxResultSize', '2G'))
sc = SparkContext(conf = conf)

#we use the merged data from hive before
regdata = sc.textFile('hdfs:///ArsMing276/NYU_TAXI_OUT/*')

##parse lines from regression data, target is total less than toll, predictor is trip time, skip the header rows
def parseData(line):
    words = [word for word in line.replace(',', ' ').split(' ')]
    try:
        y = float(words[-1]) - float(words[-2])
        x = float(words[-3])
        return LabeledPoint(y, x)
    except ValueError:
        pass
   
parsedData = regdata.map(parseData)

# Build the model
model = LinearRegressionWithSGD.train(parsedData, iterations=200, step=0.1, intercept=True)
print "weights: %s, intercept: %s" % (model.weights, model.intercept)

