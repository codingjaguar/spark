# sc is an existing SparkContext.
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row

conf = SparkConf().setAppName("sparksql")
sc = SparkContext(conf=conf)

sqlContext = SQLContext(sc)

# Load a text file and convert each line to a Row.
lines = sc.textFile("examples/src/main/resources/people.txt")
parts = lines.map(lambda l: l.split(","))
people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))

# Infer the schema, and register the DataFrame as a table.
schemaPeople = sqlContext.createDataFrame(people)
schemaPeople.registerTempTable("people")

# SQL can be run over DataFrames that have been registered as a table.
teenagers1 = sqlContext.sql("SELECT name FROM people WHERE 1 <= age AND age <= 100")
teenagers1.collect()
teenagers1.cache()
teenagers1.collect()

# Load a text file and convert each line to a Row.
lines1 = sc.textFile("examples/src/main/resources/people.txt")
parts1 = lines1.map(lambda l: l.split(","))
people1 = parts1.map(lambda p: Row(name=p[0], age=int(p[1])))
schemaPeople1 = sqlContext.createDataFrame(people1)
schemaPeople1.registerTempTable("people1")
teenagers2 = sqlContext.sql("SELECT name FROM people1 WHERE age < 1")
# The results of SQL queries are RDDs and support all the normal RDD operations.
teenagers2.collect()
