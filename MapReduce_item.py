from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as max_,count,sum,udf,round
from datetime import datetime
from pyspark.sql.types import DateType
import time


begintime = time.time()

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

salesPrelim = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("file:///SparkCourse/Liquor Sales/Liquor_Sales.csv")
    
print("The inferred schema:")
salesPrelim.printSchema()


salesPrelim.show()


print("Display Selected columns:")
dffinal = salesPrelim.select("Date","Item Number","Sale (Dollars)")
dffinal.show()

func =  udf (lambda x: datetime.strptime(x, '%m/%d/%Y'), DateType())

total = dffinal.withColumn('dt', func(col('Date')))

total.show()


print("Group by item number")
finaldf = total.groupBy("Item Number").agg(round(sum("Sale (Dollars)"),2),count("Item Number"),max_("dt"))

final = finaldf.withColumnRenamed("round(sum(Sale (Dollars)), 2)","Monetary")\
        .withColumnRenamed("count(Item Number)","Frequency")\
        .withColumnRenamed("max(dt)","Recent_Date")

final.show()

df = final.toPandas()
df.to_csv('Liquor_sales_reduced_items.csv')
finaltime = time.time()

executionTime = finaltime-begintime

print('Time to run script: ' + str(executionTime))


spark.stop()

