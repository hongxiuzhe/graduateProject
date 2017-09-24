#coding:utf-8
import numpy as np
import json
import matplotlib.pyplot as plt
import operator


data = []
index_latlng = {}
index = 0
with open('../funczone/station_visitNum_timeSerie_403') as f:
    for line in f.readlines():
        float_data = [float(flow) for flow in eval(line)[1]]
        key = eval(line)[0]
        data.append(float_data)
        index_latlng[index] = key
        index += 1
data = np.array(data)
visit_sum = data[:, 0:24].sum(axis = 1).tolist()

station_visit_num = {}
for num in visit_sum:
    key = int(num)
    if station_visit_num.has_key(key):
        station_visit_num[key] = 1 + station_visit_num[key]
    else:
        station_visit_num[key] = 1

num_sorted = sorted(station_visit_num.items(), lambda x, y: cmp(x[0], y[0]))
x = []
y = []
for key in num_sorted:
    # if key <= 300:
    x.append(key[0])
    y.append(key[1])
x_arr = np.array(x)
y_arr = np.array(y)
y_pro = y_arr * 1.0 / sum(y_arr)
y_pro_cdf = np.cumsum(y_pro)
# plt.xlim((1, 220))
plt.grid(True)
plt.title("CDF for visit user number of station")
plt.xlabel("visit user num")
plt.ylabel("probality")
plt.plot(x_arr, y_arr)
plt.show()
