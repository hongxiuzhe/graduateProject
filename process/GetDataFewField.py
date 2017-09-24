from pyspark import SparkContext
from pyspark import SQLContext,Row
import time
import re

if __name__ == "__main__":
    sc = SparkContext(appName="get data 404")
    sqlContext = SQLContext(sc)
    df = sqlContext.read.json("/input/wangtong/updatefile_final404")
    df.registerTempTable("table1")
    sqlContext.sql("select btime, etime, imsi, (downoct + upoct) as traffic, eci from table1").write.json("/input/zp/data_404.json")
