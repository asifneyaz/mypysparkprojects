#This is a spark program to find the largest spending customer from repository of ord#ers. 
# Thiscontains customer id and amount spent for multiple orders 
#made by same/different customer

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("customer-orders")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    customerID = int(fields[0])
    amount = float(fields[2])
    return (customerID, amount)

lines = sc.textFile("file:///usr/techbox/codes/mysparkprojects/customer-orders.csv")
parsedLines = lines.map(parseLine)
customerAmounts = parsedLines.reduceByKey(lambda x,y: (x+y)) 
customerAmountsSorted = customerAmounts.map(lambda x: (x[1], x[0])).sortByKey()
results = customerAmountsSorted.collect();
for result in results:
    print(result)
