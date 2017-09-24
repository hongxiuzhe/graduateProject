# -*- coding:utf8 -*-
__author__ = 'zhangpeng'

from pyspark import SparkContext
from pyspark import SQLContext,Row
import pandas as pd

# 把duation离散到对数区间
def durToSegment(row):
    dur_ms = pd.Period(row.etime, "L") - pd.Period(row.btime, "L")
    segment = [1, 10, 100, 1000, 10000, 100000, 1000000]
    index = 0
    while index + 1 < len(segment) and dur_ms > segment[index + 1]:
        index += 1
    key = dur_ms / segment[index] * segment[index]
    return (key, 1)


if __name__ == "__main__":

    sc = SparkContext(appName="flow duration static")
    sqlContext = SQLContext(sc)

    inputPath = ['/mobility/403_new.json', '/mobility/404_new.json', '/mobility/413_new.json']
    outputPath = ['/mobility/403_flow_dur2segment_log', '/mobility/404_flow_dur2segment_log', '/mobility/413_flow_dur2segment_log']
    # for i in range(1):
        # 统计每个segment的次数
    durCout_rdd = sqlContext.read.json(inputPath[1]).rdd.repartition(1000).map(durToSegment).reduceByKey(lambda x, y : x + y).map(lambda line : Row(duration = line[0], count = line[1]))
        # 转换成json
    durCout_df = sqlContext.createDataFrame(durCout_rdd)
        # 保存到HDFS中
    durCout_df.repartition(1).write.json(outputPath[1])