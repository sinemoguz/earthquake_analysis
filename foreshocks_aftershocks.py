# For the earthquake that`s magnitude is grater than 4.0, find the list of foreshocks and aftershocks within 20 km.
# -*- coding: utf-8 -*-
from pyspark import SparkConf, SparkContext
from datetime import datetime
from math import sin, cos, sqrt, atan2, radians

sparkConf = SparkConf().setMaster("local[*]").setAppName("Spark 1")
sc = SparkContext(conf = sparkConf)

logger = sc._jvm.org.apache.log4j
logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)    

def getShockType(line):
    if line[4] > 4.0:
        return (line, "mainshock")
    else:
        return (line, "foreshock or aftershock")

def filterMainShocks(line):
    return "mainshock" in line   

def filterForeAfterShocks(line):
    return "foreshock or aftershock" in line 
            
def getEarthquakeData(line):
    line = line.split('\t')
    eq = line[0]
    eqDatetime = line[1]
    latitude = line[4]
    longitude = line[5]    
    magnitude = float(line[7])  
    center = line[14].encode('ASCII', 'ignore')
    return (eq, eqDatetime, latitude, longitude, magnitude, center)   

   
def getDistance(lat1, lat2, lon1, lon2):
# approximate radius of earth in km
    R = 6373.0
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c
    return distance

def getTimeDifference(eqDateTime1, eqDateTime2): 
    timeDiff =  (eqDateTime2- eqDateTime1).total_seconds() /86400
    return timeDiff 

def findRelatedEartquake(pair):
    eq1= pair[0][0]
    eq2= pair[1][0]
    eqDateTime1 = datetime.strptime(eq1[1], '%Y%m%d%H%M%S')
    lat1 = radians(float(eq1[2]))
    lon1 = radians(float(eq1[3]))

    eqDateTime2 = datetime.strptime(eq2[1], '%Y%m%d%H%M%S')
    lat2 = radians(float(eq2[2]))
    lon2 = radians(float(eq2[3]))

    timeDiff = getTimeDifference(eqDateTime1, eqDateTime2)
    distance = getDistance(lat1, lat2, lon1, lon2)
    
    note = ""
    
    if distance < 20:
        if -2 < timeDiff <= 0:
            note = "It is a foreshock"
        elif 0 < timeDiff < 365:
            note = "It is an aftershock"
        else:
            note = "Not related"
    
    return (eq1,eq2, timeDiff, distance, note)
 
def getAfterShocks(line):
    return "It is an aftershock" in line
 
def getForeShocks(line):
    return "It is a foreshock" in line
 
#Take earthquakes data and extract header
earthquakes = sc.textFile("19000101_20160507_3.5_9.0_54_950.txt").sample(False, 0.1)
headerR = earthquakes.first()
earthquakes = earthquakes.filter(lambda x: x!= headerR).map(getEarthquakeData).map(getShockType)

mainShocks = earthquakes.filter(filterMainShocks)
possibleForeAfter = earthquakes.filter(filterForeAfterShocks)

relatedEartquakes = mainShocks.cartesian(possibleForeAfter).map(findRelatedEartquake)

foreShocks = relatedEartquakes.filter(getForeShocks)
afterShocks = relatedEartquakes.filter(getAfterShocks)


print foreShocks.take(1)
print afterShocks.take(1)

print foreShocks.saveAsTextFile("Fore Shocks")
print afterShocks.saveAsTextFile("After Shocks")

print "finished"
