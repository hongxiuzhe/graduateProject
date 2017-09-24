from pyspark import SparkContext
from pyspark import SQLContext,Row

def StationNum(row):
    traj = row.traj
    station_num = []
    for point in traj:
        station_num.append((point["station"], 1))
    return station_num

sc = SparkContext(appName="station visit num")
sqlContext = SQLContext(sc)

traj = sqlContext.read.json("/input/zp/user_traj_403.json")

station_visit_num = traj.rdd.flatMap(StationNum).reduceByKey(lambda x, y: x + y)\
.map(lambda line : Row(station = line[0], num = line[1]))

sqlContext.createDataFrame(station_visit_num).repartition(1).write.json("/input/zp/station_visit_num_403.json")