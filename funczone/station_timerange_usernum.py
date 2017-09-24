#coding:utf-8
import numpy as np
from sklearn import mixture

data = []
index_latlng = {}
index = 0
with open('station_visitNum_timeSerie_403') as f:
    for line in f.readlines():
        float_data = [float(flow) for flow in eval(line)[1]]
        key = eval(line)[0]
        data.append(float_data)
        index_latlng[index] = key
        index += 1
data = np.array(data)

res0 = open('station_usernum_0-6', 'w')
res1 = open('station_usernum_7-20', 'w')
res2 = open('station_usernum_21-24', 'w')

sum0 = data[:, 0:7].sum(axis = 1).tolist()
sum1 = data[:, 7:21].sum(axis = 1).tolist()
sum2 = data[:, 21:25].sum(axis = 1).tolist()

for i in range(len(index_latlng)):
    res0.write(index_latlng[i] + "," + str(sum0[i]) + "\n")
    res1.write(index_latlng[i] + "," + str(sum1[i]) + "\n")
    res2.write(index_latlng[i] + "," + str(sum2[i]) + "\n")
print data