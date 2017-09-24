from pyspark import SparkContext
from pyspark import SQLContext,Row

sc = SparkContext(appName="user visit station num 403")
sqlContext = SQLContext(sc)
sqlContext.read.json("/input/zp/user_traj_403.json").\
    rdd.\
    map(lambda row : str(row.imsi) + "," +  str(len(row.traj))).\
    repartition(1).\
    saveAsTextFile("/input/zp/user_visit_station_num_403.txt")

