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
results = customerAmounts.collect();
for result in results:
    print(result)
