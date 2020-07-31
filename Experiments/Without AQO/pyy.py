import json
import os
import psycopg2
import subprocess
import sys
import matplotlib.pyplot as plt
import numpy as np

sqlpath = "/home/yerzh/aqo/JOB_Queries/"

print("Hello!")
onlyfiles = [f for f in os.listdir(sqlpath) if os.path.isfile(os.path.join(sqlpath, f))]
onlyfiles.sort()
con = psycopg2.connect("dbname='imdbload'")
if len(sys.argv) > 1:
    onlyfiles = sys.argv[1:]
dictt = {}
for filename in onlyfiles:
    f = open(sqlpath + filename, "r")
    print("Use file", sqlpath + filename)
    query = f.read()
    query = "EXPLAIN (ANALYZE ON, VERBOSE ON, FORMAT JSON) " + query
    f.close()
    cur = con.cursor()
    cur.execute(query)
    dictt[filename] = {}
    result = cur.fetchone()[0][0]
    dictt[filename]['whole'] = [result['Plan']['Plan Rows'], result['Plan']['Actual Rows']]
    dictt[filename]['nodes'] = []
    temp = result["Plan"]
    while True:
        temp = temp['Plans'][0]
        dictt[filename]['nodes'].append([temp['Plan Rows'],temp['Actual Rows']])
        if 'Plans' not in temp.keys():
            break
cur.close()
con.close()

def autolabel(rects):
    """Attach a text label above each bar in *rects*, displaying its height."""
    for rect in rects:
        height = rect.get_height()
        ax.annotate('{}'.format(round(height,2)),\
                    xy=(rect.get_x() + rect.get_width() / 2, height),\
                    xytext=(0, 3),  # 3 points vertical offset\
                    textcoords="offset points",\
                    ha='center', va='bottom')

for i in dictt.keys():

    x = np.arange(len(dictt[i]['nodes']))  # the label locations
    width = 0.35  # the width of the bars
    plan_rows = []
    actual_rows = []
    for j in dictt[i]['nodes']:
        plan_rows.append(np.log(j[0]))
        actual_rows.append(np.log(j[1]))

    fig, ax = plt.subplots()
    rects1 = ax.bar(x - width/2, actual_rows, width, label='actual_rows')
    rects2 = ax.bar(x + width/2, plan_rows, width, label='plan_rows')
    ax.set_ylabel('Cardinality')
    ax.set_title('Cardinality estimation on nodes(log)')
    ax.set_xticks(x)
    ax.legend()

    autolabel(rects1)
    autolabel(rects2)

    fig.tight_layout()

    fig.savefig('/home/yerzh/aqo/'+i+'(log).png')
    plt.close(fig)


for i in dictt.keys():

    x = np.arange(len(dictt[i]['nodes']))  # the label locations
    width = 0.35  # the width of the bars
    plan_rows = []
    actual_rows = []
    for j in dictt[i]['nodes']:
        plan_rows.append(j[0])
        actual_rows.append(j[1])

    fig, ax = plt.subplots()
    rects1 = ax.bar(x - width/2, actual_rows, width, label='actual_rows')
    rects2 = ax.bar(x + width/2, plan_rows, width, label='plan_rows')
    ax.set_ylabel('Cardinality')
    ax.set_title('Cardinality estimation on nodes')
    ax.set_xticks(x)
    ax.legend()

    autolabel(rects1)
    autolabel(rects2)

    fig.tight_layout()

    fig.savefig('/home/yerzh/aqo/'+i+'.png')
    plt.close(fig)
