from pyspark import SparkContext
from pyspark import SQLContext,Row
import time
import re

def timeFormat(str):
    start_second = time.mktime(time.strptime("2015-04-13 00:00:00.000".split(".")[0], '%Y-%m-%d %H:%M:%S'))
    second = time.mktime(time.strptime(str.split(".")[0], '%Y-%m-%d %H:%M:%S'))
    time_interval_index = int((second - start_second) / (15 * 60))
    return time_interval_index

def stationAndTimeSegment(row):
    traj = row.traj
    station_visit = []
    for stay in traj:
        station_visit.append((stay["station"] + "#" + str(timeFormat(stay["btime"])), 1))
    return station_visit

def staionVisitTimeSerie(iter):
    tmp = list(iter)
    iter = tmp
    timeSerie = [0]* (60 / 15 * 24)
    for e in iter:
        timeSerie[int(e.split("#")[0])] = int(e.split("#")[1])
    return timeSerie

if __name__ == "__main__":
    sc = SparkContext(appName="station visit num time serie 413")
    sqlContext = SQLContext(sc)

    traj = sqlContext.read.json("/input/zp/user_traj_413.json")
    traj.rdd.flatMap(stationAndTimeSegment).reduceByKey(lambda x, y: x + y)\
    .map(lambda tuple: (tuple[0].split("#")[0], tuple[0].split("#")[1] + "#" + str(tuple[1])))\
    .groupByKey()\
    .mapValues(staionVisitTimeSerie)\
    .repartition(1) \
    .saveAsTextFile("/input/zp/station_visitNum_timeSerie_413")



