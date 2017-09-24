from pyspark import SparkContext
from pyspark import SQLContext,Row
from pyspark import StorageLevel

sc = SparkContext(appName="hits no dur")
sqlContext = SQLContext(sc)

def userStationNum(row):
    traj = row.traj
    station_num = []
    for point in traj:
        station_num.append((row.imsi + "#" + point["station"], 1))
    return station_num

def CalculateStation(line):
    user_val_map = user_broad.value
    imsi = line[0].split("#")[0]
    station = line[0].split("#")[1]
    visit_num = line[1]
    return (station, visit_num * user_val_map[imsi])

def CalculateUser(line):
    station_val_map = station_broad.value
    imsi = line[0].split("#")[0]
    station = line[0].split("#")[1]
    visit_num = line[1]
    return (imsi, visit_num * station_val_map[station])

def list2map(l, n):
    m = {}
    for e in l:
        m[e[0]] = e[1] * 1.0 / n * 100
    return m


user_station_num = sqlContext.read.json("/input/zp/user_traj_403.json").rdd.flatMap(userStationNum)\
    .reduceByKey(lambda x, y: x + y).persist(StorageLevel(False, True, False, False, 1))

user_val = user_station_num.map(lambda line: (line[0].split("#")[0], 100))\
    .reduceByKey(lambda x, y : x).collect()
user_val_map = list2map(user_val, 100)
user_broad = sc.broadcast(user_val_map)
station_val = user_station_num.map(lambda line: (line[0].split("#")[1], 100))\
    .reduceByKey(lambda x, y : x).collect()
station_val_map = list2map(station_val, 100)
station_broad = sc.broadcast(station_val_map)

for i in range(30):
    station_val = user_station_num.map(CalculateStation).reduceByKey(lambda x, y: x + y).collect()
    station_val_max = 0
    for v in station_val:
        if v[1] > station_val_max:
            station_val_max = v[1]
    station_val_map = list2map(station_val, station_val_max)
    station_broad = sc.broadcast(station_val_map)

    user_val = user_station_num.map(CalculateUser).reduceByKey(lambda x, y: x + y).collect()
    user_val_max = 0
    for v in user_val:
        if v[1] > user_val_max:
            user_val_max = v[1]
    user_val_map = list2map(user_val, user_val_max)
    user_broad = sc.broadcast(user_val_map)

station_res = []
for station in station_val_map.keys():
    station_res.append(station + "," + str(station_val_map[station]))
hits_res = sc.parallelize(station_res).map(lambda line: Row(station = line.split(",")[0], val = line.split(",")[1]))
sqlContext.createDataFrame(hits_res).repartition(1).write.json("/input/zp/hits_no_dur.json")