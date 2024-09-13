from pyspark import SparkContext
import os

from pyspark.sql.functions import date_add

# Set the path to the Python interpreter
os.environ["PYSPARK_PYTHON"] = "C:/Users/kprat/Documents/Python/Python/Python37/python.exe"

# Initialize Spark context
sc = SparkContext("local[*]", "sparkrdd")
sc.setLogLevel("ERROR")

# Path to the local CSV file
file_path = "C:/Users/kprat/Documents/DataFiles/orders.csv"

# Read the CSV file into an RDD
orders_rdd = sc.textFile(file_path)

# Extract the header
header = orders_rdd.first()

# Filter out the header and take the first 5 lines of data
data_rdd = orders_rdd.filter(lambda line: line != header)

#1.count of orders in each status
mapped_rdd =data_rdd.map(lambda x:(x.split(",")[3],1))
reduced_rdd =mapped_rdd.reduceByKey(lambda x,y:x+y)
reduced_sorted =reduced_rdd.sortBy(lambda x:x[1],False)

#2.Top 10 customers with most number of orders
customers_mapped=data_rdd.map(lambda x:(x.split(",")[2],1))
customers_reduced=customers_mapped.reduceByKey(lambda x,y:x+y)
premium_customers =customers_reduced.sortBy(lambda x:x[1],False)

#3.Distinct count of customers who placed at-least one order
distinct_customers =orders_rdd.map(lambda x:x.split(",")[2]).distinct()
Final_count= distinct_customers.count()
print(Final_count)

#4.Customer with maximum no of closed orders
filtered_rdd= orders_rdd.filter(lambda x:(x.split(",")[3]=='CLOSED'))
filtered_mapped= filtered_rdd.map(lambda x:(x.split(",") [2],1))
filtered_aggregated= filtered_mapped.reduceByKey(lambda x,y:x+y)
sorted_aggregated= filtered_aggregated.sortBy(lambda x:x[1],False)
sample_data=sorted_aggregated
for line in sample_data.take(1):
    print(line)

# Stop the Spark context
sc.stop()
