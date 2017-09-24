# -*- coding:utf8 -*-
import numpy as np
import json
import matplotlib.pyplot as plt
import operator

#key是用户访问基站次数，value是这个次数的频次
user_visit_num = {}

file = open("user_visit_station_num_403.txt", 'r')
for line in file.readlines():
    key = int(line.split(",")[1])
    if(user_visit_num.has_key(key)):
        # print key, user_visit_num.get(key)
        user_visit_num[key] = 1 + user_visit_num.get(key)
    else:
        user_visit_num[key] = 1
num_sorted = sorted(user_visit_num.items(), lambda x, y: cmp(x[0], y[0]))
x = []
y = []
for key in num_sorted:
    x.append(key[0])
    y.append(key[1])
x_arr = np.array(x)
y_arr = np.array(y)
y_pro = y_arr * 1.0 / sum(y_arr)
y_pro_cdf = np.cumsum(y_pro)
plt.xlim((1, 220))
plt.grid(True)
plt.title("CDF for visit station number of user")
plt.xlabel("visit station num")
plt.ylabel("probality")
plt.plot(x_arr, y_pro_cdf)
plt.show()