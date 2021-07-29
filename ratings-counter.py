#Build a histogram from movie ratings
#Example given below
#1 5500
#2 4250
#3 12500
#4 22000
#5 16545
#my first python code
from pyspark import SparkSession
import collections
spark = (SparkSession.builder.appName("ratings"))

#conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
#sc = SparkContext(conf=conf)

lines = spark.textFile("file:////home/datalakedata/mysparkprojects/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))

