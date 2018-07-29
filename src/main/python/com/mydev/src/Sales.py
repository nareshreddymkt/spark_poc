
from pyspark.sql import *
print("hellooo")

spark = SparkSession.builder.master("local").appName("Word Count").getOrCreate()
print(spark)