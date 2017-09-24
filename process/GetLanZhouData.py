from pyspark import SparkContext
from pyspark import SQLContext,Row
from pyspark.sql.types import *

def filter(row):
    eci2grid_set = eci2grid_broad.value
    eci = row.eci
    iskeep = eci2grid_set.__contains__(eci) and row.eci != None and row.btime != None and row.etime != None and row.imsi != None
    return iskeep

sc = SparkContext(appName="get lanzhou 413 data")
sqlContext = SQLContext(sc)

eci2grid = sc\
    .textFile("/input/zp/eci2grid.txt")\
    .map(lambda line: line.split(",")[0]).collect()
eci2grid_broad = sc.broadcast(set(eci2grid))

schemaString = "btime eci etime imsi traffic"
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

data_lanzhou_rdd = sqlContext.read\
    .json("/input/zp/data_413.json")\
    .rdd.filter(filter)

sqlContext.createDataFrame(data_lanzhou_rdd, schema)\
    .write\
    .json("/input/zp/data_lanzhou_413.json")
