# sparksqlJointest.py
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, Row

# setup the location enviroment
dataDir = "test/data"
table1Name = "infoTable"
table2Name = "cityTable"

table1Loc = dataDir + "/" + table1Name
table2Loc = dataDir + "/" + table2Name

# set up spark conf
conf = SparkConf().setAppName("JoinTest")
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

# register the firstTable
# load a text file and convret each line to a row
lines = sc.textFile(table1Loc)
parts = lines.map(lambda l : l.split(","))
table1 = parts.map(lambda p : Row(id = int(p[0]), first_name = p[1], last_name = p[2], email = p[3],
    country = p[4], ip_address = p[5], cityId = int(p[6])))

# Infer the schema, and register the DataFrame as a table.
schemaTable1 = sqlContext.createDataFrame(table1)
schemaTable1.registerTempTable("table1")

# the other table
lines2 = sc.textFile(table2Loc)
parts2 = lines.map(lambda l : l.split(","))
table2 = parts2.map(lambda p : Row(cityId = int(p[0]), city = p[1]))
schemaTable2 = sqlContext.createDataFrame(table2)
schemaTable2.registerTempTable("table2")

# run the sql
data = sqlContext.sql("SELECT table1.first_name from table1 JOIN table2 on table1.cityId = table2.cityId where table1.id < 2000 AND table1.cityId < 20")

# cache the result
data.cache()
# print the result
name = data.map(lambda p: "FName: " + p.first_name)
for theName in name.collect():
    print(theName)


data2 = sqlContext.sql("SELECT table1.first_name from table1 JOIN table2 on table1.cityId = table2.cityId where table1.id < 2000")
data2.collect()
data3 = sqlContext.sql("SELECT table1.first_name from table1 JOIN table2 on table1.cityId = table2.cityId where table1.id < 1000")
data3.collect()
