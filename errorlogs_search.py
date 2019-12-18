#this is a spark program to search for errors in log files
from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("SearchForErrorsInLogFiles")
sc = SparkContext(conf = conf)

logfilesrdd = sc.textFile("file:///var/log/hadoop/hdfs/hadoop-hdfs-*")
onlyerrorsrdd = logfilesrdd.filter(lambda line: "ERROR" in line)
onlyerrorsrdd.saveAsTextFile("file:///usr/techbox/codes/mysparkprojects/onlyerrors")



