# Find the closest (by time and by distance) two earthquakes with the highest magnitudes occurred between 1900 and 2015
#earthquakes -> magnitudeOfFirstEarchquake * magnitudeOfSecondEarchquake  / distance * time difference

# -*- coding: utf-8 -*-
from pyspark import SparkConf, SparkContext
from datetime import datetime
from math import sin, cos, sqrt, atan2, radians


sparkConf = SparkConf().setMaster("local[*]").setAppName("Spark 1")
sc = SparkContext(conf = sparkConf)

logger = sc._jvm.org.apache.log4j
logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)

#get earthquake data in pairs
def getEarthquakeData(line):
    line = line.split('\t')
    eq = line[0]
    eqDatetime = line[1]
    latitude = line[4]
    longitude = line[5]    
    magnitude = float(line[7])  
    center = line[14]
    return (eq, eqDatetime, latitude, longitude, magnitude, center)   

#eliminate self cartesian results
def ignoreSelf(pairlist):
    return not pairlist[0][0] == pairlist[1][0]
 
def getTimeDifference(eqDateTime1, eqDateTime2): 
    timeDiff =  (eqDateTime1 - eqDateTime2).total_seconds()
    return timeDiff 

def getDistance(lat1, lat2, lon1, lon2):
# approximate radius of earth in km
    R = 6373.0
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c
    return distance 

def closestEarthquake(pair):
    eq1= pair[0]
    eq2= pair[1]  
    eqDateTime1 = datetime.strptime(eq1[1], '%Y%m%d%H%M%S')
    lat1 = radians(float(eq1[2]))
    lon1 = radians(float(eq1[3]))
    magn1 = eq1[4]
    
    eqDateTime2 = datetime.strptime(eq2[1], '%Y%m%d%H%M%S')
    lat2 = radians(float(eq2[2]))
    lon2 = radians(float(eq2[3]))
    magn2 = eq2[4]
    
    timeDiff = getTimeDifference(eqDateTime1, eqDateTime2)
    distance = getDistance(lat1, lat2, lon1, lon2)
    try:
        closest = magn1*magn2 / distance * timeDiff
    except ZeroDivisionError:
        closest = 0.0
    return (abs(closest),pair)

def cleanZeros(line):
    return not 0.0 in line
#Take earthquakes data and extract header
earthquakes = sc.textFile("19000101_20160507_3.5_9.0_54_950.txt").sample(False, 0.1)
headerR = earthquakes.first()
earthquakes = earthquakes.filter(lambda x: x!= headerR).map(getEarthquakeData)

closestEqs = earthquakes.cartesian(earthquakes).filter(ignoreSelf).map(closestEarthquake).sortByKey().filter(cleanZeros)

print closestEqs.take(1)
print closestEqs.saveAsTextFile("closestEqs")
print "finished"