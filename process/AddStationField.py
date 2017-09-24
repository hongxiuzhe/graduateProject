from pyspark import SparkContext
from pyspark import SQLContext,Row
from pyspark.sql.types import *

def addField(row):
    return Row(imsi = row.imsi, btime = row.btime, etime = row.etime, station = eci_station_broad.value.get(row.eci), eci = row.eci, traffic = row.traffic)

sc = SparkContext(appName="add station field 413")
sqlContext = SQLContext(sc)

eci2grid = sc\
    .textFile("/input/zp/eci2grid.txt")\
    .map(lambda line: line.split(",")).collect()
eci_station_map = {}
for line in eci2grid:
    eci_station_map[line[0]] = line[3] + "|" + line[4]
eci_station_broad = sc.broadcast(eci_station_map)

data_lanzhou_rdd = sqlContext.read\
    .json("/input/zp/data_lanzhou_413.json")\
    .rdd.map(addField)

sqlContext.createDataFrame(data_lanzhou_rdd)\
    .write\
    .json("/input/zp/data_lanzhou_413_new.json")