import json
import os
import psycopg2
import subprocess
import sys
import numpy as np
import pandas as pd

sqlpath = "/home/yerzh/aqo/JOB_Queries/"
dictt = {}

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
    for i in range(4):
        cur.execute("SET aqo.mode = 'learn';")
        cur.execute(query)
        records = cur.fetchone()
        con.commit()
    cur.execute("SET aqo.mode = 'controlled';")
    cur.execute(query)
    dictt[filename] = {}
    result = cur.fetchone()[0][0]
    con.commit()
    dictt[filename]['whole'] = [result['Plan']['Plan Rows'], result['Plan']['Actual Rows']]
    dictt[filename]['nodes'] = []
    temp = result["Plan"]
    while True:
        temp = temp['Plans'][0]
        dictt[filename]['nodes'].append([temp['Plan Rows'],temp['Actual Rows']])
        if 'Plans' not in temp.keys():
            break
    cur.close()
    if os.path.isfile('/home/yerzh/aqo/data.csv'):
        df = pd.read_csv('/home/yerzh/aqo/data.csv', index_col=[0])
    else:
        df = pd.DataFrame(columns=['AQO(RIDGE)','Actual'])
    plan_rows = []
    actual_rows = []
    for j in dictt[filename]['nodes']:
        plan_rows.append(j[0])
        actual_rows.append(j[1])
    df = df.append(pd.DataFrame(np.array([plan_rows, actual_rows]).T, columns=['AQO(RIDGE)','Actual'], index=[filename for t in plan_rows]))
    df.to_csv('/home/yerzh/aqo/data.csv')
    os.remove(sqlpath + filename)
con.close()
