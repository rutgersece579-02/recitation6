from pyspark import SparkContext,SparkConf

conf=SparkConf()
conf.setAppName("test application")

sc=SparkContext(conf=conf)

logFile=sc.textFile("test.txt")

counts = logFile.flatMap(lambda line: ____________).___(lambda word: (________)).reduceByKey(lambda ___________)
counts.saveAsTextFile("out")
