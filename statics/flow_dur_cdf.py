# -*- coding:utf8 -*-
__author__ = 'zhangpeng'

# 统计flow duration的cdf分布图

import numpy as np
import json
import matplotlib.pyplot as plt
import operator


durCout_tuple_list = []
durCout_tuple_list2 = []
x_dur = []
y_cout = []
x_dur2 = []
y_cout2 = []

# 从json文件读数据
file = open('flowDurCDF403','r')
for line in file.readlines():  # 读取json字符串
    dur_count_dict = json.loads(line)             # json字符串转化成字典
    durCout_tuple_list.append((dur_count_dict["duration"], dur_count_dict["count"]))  # list的元素为元组
# 从json文件读数据
file2 = open('flowDurCDF404','r')
for line in file2.readlines():  # 读取json字符串
    dur_count_dict2 = json.loads(line)             # json字符串转化成字典
    durCout_tuple_list2.append((dur_count_dict2["duration"], dur_count_dict2["count"]))  # list的元素为元组


# 按元组第一个元素排序
durCout_tuple_list.sort(key = operator.itemgetter(0))
durCout_tuple_list2.sort(key = operator.itemgetter(0))

# 排序后取出x和y
for item in durCout_tuple_list:
    x_dur.append(item[0])
    y_cout.append(item[1])
for item in durCout_tuple_list2:
    x_dur2.append(item[0])
    y_cout2.append(item[1])

# list转成array
x_dur_arr = np.array(x_dur)
y_cout_arr = np.array(y_cout)
# 计算概率
y_pro_arr = y_cout_arr * 1.0 / sum(y_cout_arr)
# 计算累计概率
y_cumpro_arr = np.cumsum(y_pro_arr)
# list转成array
x_dur_arr2 = np.array(x_dur2)
y_cout_arr2 = np.array(y_cout2)
# 计算概率
y_pro_arr2 = y_cout_arr2 * 1.0 / sum(y_cout_arr2)
# 计算累计概率
y_cumpro_arr2 = np.cumsum(y_pro_arr2)

# 画出cdf分布图
fig1 = plt.figure(1) # 创建图表1
ax1 = fig1.add_subplot(111) # 创建子图1
ax1.semilogx(x_dur_arr, y_cumpro_arr, "bo")
# ax1.semilogx(x_dur_arr2, y_cumpro_arr2, "r+")
ax1.grid(True)
ax1.grid(which='both', axis='x')
ax1.set_xlim(0,1000000)
ax1.set_ylim(0,1.01)

plt.title("CDF of flow durations")
plt.xlabel("Duration of flows(Millisecond)")
plt.ylabel("Percentage")
plt.show()

