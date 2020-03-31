import findspark
findspark.init('C:\spark-3.0.0-preview2-bin-hadoop2.7')

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("missingdata").getOrCreate()

df = spark.read.csv("ContainsNull.csv",header=True,inferSchema=True)
df.show()

# Drop
df.na.drop().show()
df.na.drop(thresh=2).show() # the row at least should have 2 null values to show up 
df.na.drop(subset=["Sales"]).show()
df.na.drop(how='any').show()
df.na.drop(how='all').show()

# Fill with "string"
df.na.fill('NEW VALUE').show()
df.na.fill(0).show()
df.na.fill('No Name',subset=['Name']).show()

# Fill with mean
from pyspark.sql.functions import mean
mean_val = df.select(mean(df['Sales'])).collect()
mean_val[0][0]
mean_sales = mean_val[0][0]
df.na.fill(mean_sales,["Sales"]).show()
