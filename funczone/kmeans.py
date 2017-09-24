from sklearn.cluster import KMeans
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

key_data = {}
data = []
index_latlng = {}
with open('station_visitNum_timeSerie_403') as f:
    for line in f.readlines():
        float_data = [float(flow) for flow in eval(line)[1]]
        key = eval(line)[0]
        # data.append(float_data)
        key_data[key] = float_data
with open('station_visitNum_timeSerie_413') as f2:
    for line in f2.readlines():
        float_data = [float(flow) for flow in eval(line)[1]]
        key = eval(line)[0]
        if key_data.has_key(key):
            key_data[key].extend(float_data)
        # data.append(float_data)
index = 0
with open('station_visitNum_timeSerie_404') as f:
    for line in f.readlines():
        float_data = [float(flow) for flow in eval(line)[1]]
        key = eval(line)[0]
        if key_data.has_key(key):
            key_data[key].extend(float_data)
            if len(key_data[key]) == 288 :
                print "length is" + str(len(key_data[key]))
                index_latlng[index] = eval(line)[0]
                index += 1
                data.append(key_data[key])

data = np.array(data)    #  (1076, 96)

n_clusters = 3
kmeans = KMeans(n_clusters, random_state=1).fit(data)


labels = kmeans.labels_

label0_index = []
label1_index = []
label2_index = []
# label3_index = []
# label4_index = []
# label5_index = []


cluster0 = open('cluster0', 'w')
cluster1 = open('cluster1', 'w')
cluster2 = open('cluster2', 'w')

for index,label in enumerate(labels):
    if label == 0:
        label0_index.append(index)
        cluster0.write(index_latlng[index] + '\n')
    if label == 1:
        label1_index.append(index)
        cluster1.write(index_latlng[index] + '\n')
    if label == 2:
        label2_index.append(index)
        cluster2.write(index_latlng[index] + '\n')
    # if label == 3:
    #     label3_index.append(index)
    # if label == 4:
    #     label4_index.append(index)
    # if label == 5:
    #     label5_index.append(index)
cluster0.close()
cluster1.close()
cluster2.close()

num_label0 = len(label0_index)
num_label1 = len(label1_index)
num_label2 = len(label2_index)
# num_label3 = len(label3_index)
# num_label4 = len(label4_index)
# num_label5 = len(label5_index)

agg_label0 = data[label0_index].mean(axis = 0)
agg_label1 = data[label1_index].mean(axis = 0)
agg_label2 = data[label2_index].mean(axis = 0)
# agg_label3 = data[label3_index].mean(axis = 0)
# agg_label4 = data[label4_index].mean(axis = 0)
# agg_label5 = data[label5_index].mean(axis = 0)

plt.plot(agg_label0, label='0')
plt.plot(agg_label1, label = '1')
plt.plot(agg_label2, label = '2')
# plt.plot(agg_label3)
# plt.plot(agg_label4)
# plt.plot(agg_label5)
plt.title("visit num cluster(KMeans)")
plt.xlabel("time (15 min)")
plt.ylabel("visit num")
plt.grid(True)
plt.legend()
plt.show()






