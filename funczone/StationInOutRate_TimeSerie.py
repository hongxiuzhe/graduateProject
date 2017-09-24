
from pyspark import SparkContext
from pyspark import SQLContext,Row
import time

def timeFormat(str):
    start_second = time.mktime(time.strptime("2015-04-03 00:00:00.000".split(".")[0], '%Y-%m-%d %H:%M:%S'))
    second = time.mktime(time.strptime(str.split(".")[0], '%Y-%m-%d %H:%M:%S'))
    time_interval_index = int((second - start_second) / (15 * 60))
    return time_interval_index

def getField(row):
    trajs = row.traj
    time_station_inout_list = []
    before_station = "-1"
    for stay in trajs :
        time_index = timeFormat(stay["btime"])
        if before_station != "-1":
            time_station_inout_list.append((str(time_index) + ":" + before_station + "#" + "out", 1))
            time_station_inout_list.append((str(time_index) + ":" + stay["station"] + "#" + "in", 1))
        else:
            time_station_inout_list.append((str(time_index) + ":" + stay["station"] + "#" + "in", 1))
        before_station = stay["station"]
    # print time_station_inout_list
    return time_station_inout_list

def filter(tuple):
    tmp = list(tuple[1])
    if len(tmp) == 2:
        return True
    else:
        return False

def p(x):
    print x

def getInOutRate(iter) :
    tmp = list(iter)
    print tmp
    x = tmp[0]
    y = tmp[1]
    tag_1 = x.split("$")[0]
    rate = float(x.split("$")[1]) / float(y.split("$")[1])
    print rate
    if tag_1 == "in":
        rate = 1.0 / rate
    return str(rate)

def getRateList(x) :
    rateList = [0.0] * 24 * (60 / 15)
    for e in x[1]:
        rateList[e[0]] = e[1]
    return (x[0], rateList)

if __name__ == "__main__":
    sc = SparkContext(appName="station inout rate time serie")
    sqlContext = SQLContext(sc)

    a = sqlContext.read.json("/input/zp/user_traj_403.json").rdd.flatMap(getField) \
        .reduceByKey(lambda x, y : x + y)\
        .map(lambda pair: (pair[0].split("#")[0], pair[0].split("#")[1] + "$" + str(pair[1])))\
        .groupByKey()\
        .filter(filter)\
        .mapValues(getInOutRate)\
        .map(lambda pair: (pair[0].split(":")[1], (int(pair[0].split(":")[0]), pair[1])))\
        .aggregateByKey([], lambda x, y: x + [y], lambda x, y: x + y)\
        .map(getRateList)\
        .repartition(1)\
        .saveAsTextFile("/input/zp/station_inout_rate_timeSerie_403")

