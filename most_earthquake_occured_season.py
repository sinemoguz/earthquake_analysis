# What season of year do most earthquakes occurred in last 35 years?

from pyspark import SparkConf, SparkContext
from datetime import datetime

sparkConf = SparkConf().setMaster("local[*]").setAppName("Spark 1")
sc = SparkContext(conf = sparkConf)

logger = sc._jvm.org.apache.log4j
logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)

def filterLast35YearsData(line):
    earthquake = line.split("\t")    
    year = int(earthquake[2].split(".")[0])    
    return year > 1980

def seasonInfo(earthquake):
    earthquake = earthquake.split("\t")
    day_of_year = datetime.strptime(earthquake[2], '%Y.%m.%d').timetuple().tm_yday

    spring = range(80, 172)
    summer = range(172, 264)
    fall = range(264, 355)
    
    if day_of_year in spring:
        season = 'spring'
    elif day_of_year in summer:
        season = 'summer'
    elif day_of_year in fall:
        season = 'fall'
    else:
        season = 'winter'
      
    return (season, 1)

#Take earthquakes data and extract header
earthquakes = sc.textFile("19000101_20160507_3.5_9.0_54_950.txt")
headerR = earthquakes.first()
earthquakes = earthquakes.filter(lambda x: x!= headerR).filter(filterLast35YearsData).map(seasonInfo)\
.reduceByKey(lambda x,y: x+y).map(lambda (x,y) : (y,x)).sortByKey(False)

print earthquakes.take(10)
