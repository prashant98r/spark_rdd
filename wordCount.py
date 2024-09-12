from pyspark.sql import SparkSession
from pyspark import SparkContext
import os

os.environ[ "PYSPARK_PYTHON"] = "C:/Users/kprat/Documents/Python/Python/Python37/python.exe"
sc = SparkContext("local[*]", "sparkrdd")
sc.setLogLevel("ERROR")
rdd1 = sc.textFile("C:/Users/kprat/Documents/DataFiles/data1.txt")
rdd2 = rdd1.flatMap(lambda x:x.split(" "))
rdd3=rdd2.map( lambda x: (x, 1))
rdd4=rdd3.reduceByKey( lambda x, y: x + y)

for i in rdd4.collect():
 print(i)