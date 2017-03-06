import sys
from pyspark import SparkContext
sc=SparkContext()

file=sc.textFile("file:/home/training/Data/data.csv")

c= file.map(lambda l: l.strip().split(",")).map(lambda f: ((str(f[1]),str(f[4]),str(f[2]),str(f[3])),1)).reduceByKey(lambda v1,v2:v1+v2).sortByKey(ascending=True).saveAsTextFile("file:/home/training/output/p3.txt")

sc.stop()

