from pyspark import SparkContext
from pyspark import SQLContext,Row
from pyspark.sql.types import *

def buildTraj(points):
    tmp = list(points)
    points = tmp
    if len(points) > 1:
        points.sort(lambda x, y: cmp(x[0] ,y[0]))
        traj = [list(points[0])]
        before_point = points[0]
        for i in range(1, len(points), 1):
            point = points[i]
            if point[0] > before_point[0]:
                if point[2] == before_point[2]:
                    traj[len(traj) - 1][1] = point[1]
                    before_point = traj[len(traj) - 1]
                else:
                    traj[len(traj) - 1][1] = point[0]
                    traj.append(list(point))
                    before_point = point
            else:
                if point[2] == before_point[2]:
                    traj[len(traj) - 1][1] = max(before_point[2], point[2])
                    before_point = traj[len(traj) - 1]
                else:
                    traj[len(traj) - 1][1] = point[0]
                    traj.append(list(point))
                    before_point = point
        return traj
    return points


sc = SparkContext(appName="generate user traj 413s")
sqlContext = SQLContext(sc)

# data_lanzhou = sqlContext.read.json("/input/zp/data_lanzhou_413.json")
data_lanzhou = sqlContext.read.json("/Users/zhangpeng/work/code/python/graduateProject/data_lanzhou_404.json")

user_traj = data_lanzhou.rdd.map(lambda row: (row.imsi, (row.btime, row.etime, row.station))).groupByKey()\
    .mapValues(buildTraj)

imsi = StructField("imsi", StringType(), True)
btime = StructField("btime", StringType(), True)
etime = StructField("etime", StringType(), True)
station = StructField("station", StringType(), True)
traj = StructField("traj", ArrayType(StructType([btime, etime, station]), True), True)
fields = [imsi, traj]
schema = StructType(fields)

# sqlContext.createDataFrame(user_traj, schema).write.json("/input/zp/user_traj_413.json")

sqlContext.createDataFrame(user_traj, schema).write.json("/Users/zhangpeng/work/code/python/graduateProject/user_traj_413.json")