# -*- coding: utf-8 -*-
"""
Created on Mon Oct 26 20:55:04 2015

@author: Kruthika
"""

import requests
from pandas.io.json import json_normalize
import matplotlib.pyplot as plt
import pandas as pd
import sqlite3 as lite

r = requests.get('http://www.citibikenyc.com/stations/json')

# To get a basic view of the text
r.text
r.json()
r.json().keys()
#r.json()['executionTime']
#len(r.json()['stationBeanList'])

#Gathering all the fields in 'stationBeanList' together and storing as list
key_list = []
for station in r.json()['stationBeanList']:
    for k in station.keys():
        if k not in key_list:
            key_list.append(k)
            
#passing values associated to pandas to create a DataFrame
df = json_normalize(r.json()['stationBeanList'])

# Plot range of values for each attribute
df['availableBikes'].hist()
plt.show()

df['totalDocks'].hist()
plt.show()

#Checking if there are any test stations
df.testStation.value_counts()

#Checking the status value of stations
df.statusValue.value_counts()
df.availableBikes.mean()
df.availableBikes.median()

#Deleting the stations that are not in service and calculating mean and median
df = df[df.statusValue != 'Not In Service']
df.statusValue.value_counts()
df.availableBikes.mean()
df.availableBikes.median()

#Creating the database
con = lite.connect('citi_bike.db')
cur = con.cursor()

with con:
    cur.execute("DROP TABLE IF EXISTS citibike_reference")
    cur.execute('CREATE TABLE citibike_reference (id INT PRIMARY KEY, totalDocks INT, city TEXT, altitude INT, stAddress2 TEXT, longitude NUMERIC, postalCode TEXT, testStation TEXT, stAddress1 TEXT, stationName TEXT, landMark TEXT, latitude NUMERIC, location TEXT )')
    cur.execute('DELETE FROM citibike_reference')
# Inserting values into the table
sql = "INSERT INTO citibike_reference (id, totalDocks, city, altitude, stAddress2, longitude, postalCode, testStation, stAddress1, stationName, landMark, latitude, location) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"
#for loop to populate values in the database
with con:
    for station in r.json()['stationBeanList']:
        #id, totalDocks, city, altitude, stAddress2, longitude, postalCode, testStation, stAddress1, stationName, landMark, latitude, location)
        cur.execute(sql,(station['id'],station['totalDocks'],station['city'],station['altitude'],station['stAddress2'],station['longitude'],station['postalCode'],station['testStation'],station['stAddress1'],station['stationName'],station['landMark'],station['latitude'],station['location']))

#extract the column from the DataFrame and put them into a list
station_ids = df['id'].tolist() 

#add the '_' to the station name and also add the data type for SQLite
station_ids = ['_' + str(x) + ' INT' for x in station_ids]

#create the table
#in this case, we're concatentating the string and joining all the station ids (now with '_' and 'INT' added)
with con:
    cur.execute("DROP TABLE IF EXISTS available_bikes")
    cur.execute("CREATE TABLE available_bikes ( execution_time INT, " +  ", ".join(station_ids) + ");")

# a package with datetime objects
#import time

# a package for parsing a string into a Python datetime object
from dateutil.parser import parse 

import collections

#take the string and parse it into a Python datetime object
exec_time = parse(r.json()['executionTime'])

with con:
    cur.execute('DELETE FROM available_bikes')
    cur.execute('INSERT INTO available_bikes (execution_time) VALUES (?)', (exec_time.strftime('%Y-%m-%dT%H:%M:%S'),))
    
id_bikes = collections.defaultdict(int) #defaultdict to store available bikes by station

#loop through the stations in the station list
for station in r.json()['stationBeanList']:
    id_bikes[station['id']] = station['availableBikes']

#iterate through the defaultdict to update the values in the database
with con:
    for k, v in id_bikes.iteritems():
        cur.execute("UPDATE available_bikes SET _" + str(k) + " = " + str(v) + " WHERE execution_time = ?" , (exec_time.strftime('%Y-%m-%dT%H:%M:%S'),))
