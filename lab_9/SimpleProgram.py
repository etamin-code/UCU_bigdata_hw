import csv	
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('SimpleSparkProject').getOrCreate()

data_dict = csv.DictReader(open("data.csv"))
df = spark.createDataFrame(data_dict)

print("number of rows in df: ", df.count())

