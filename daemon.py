import json
import os,time
from nltk import Tree
import csv
import re, random

def process(string, alias):
    m = re.findall(r'\w+',string)
    if '=' in string:
        return m[0]
    else:
        return m[1]

def filter1(query, alias):

    k = alias.replace(".", "").split()
    m = re.findall(r'[' + k[0] + ']+' + '[.]' + '\w+', query)
    filterstring = ''
    counter = 0
    for n in m:
        x = n.replace(alias, "")
        if counter != len(m) - 1:
            filterstring += x + " OR "
        else:
            filterstring += x
        counter += 1
    return filterstring


createindexon = {}
createdindex = []
while 1:
    file = open("/home/ashish/Desktop/log/explain_file_match1.csv", "r+")
    csv_file = csv.reader(file)
    csvdata= []
    for r in csv_file:
        csvdata.append(r)
    file.truncate(0)
    file.close()
    if len(csvdata)==0:
        time.sleep(10)
        continue
    diction_key = {}
    for row in csvdata:
        table_name = row[0]
        alias = row[1]
        cost = float(row[2])
        filter = row[3]
        dbName = row[4]
        query = row[5]
        if len(str(filter).strip())==0:
            filter = filter1(query, alias)

        f = str(filter).split("OR")
        for and_ in f:
            ands = and_.split("AND")
            l = [dbName, table_name]
            k = []
            for a in ands:
                k.append(process(a, alias))


            someset= list(set(k))
            x = tuple(l+someset)

            if x in createindexon.keys():
                createindexon[x]['accumulativecost'] += cost
                createindexon[x]['count'] += 1
                col = str(x[2:]).replace("'", "")
                print(col)
                if col[-1] ==',':
                    col1 = col[:len(col) - 2] + col[len(col) - 1:]
                else:
                    col1 = col
                if col[-2] == ',':
                    col1 = col[:len(col) - 2] + col[len(col) - 1:]
                else:
                    col1 = col

                if createindexon[x]['accumulativecost'] > 20 and x not in createdindex:
                    cmd = "psql "+dbName+" --command=\"create index idx_"+table_name+str(random.randint(1,100000)) +" on "+table_name+col1+"\""
                    ret_val = os.system(cmd)
                    createdindex.append(x)

            else:
                createindexon[x]= {'accumulativecost':cost, 'count':1}

    print(createindexon)
    time.sleep(10)