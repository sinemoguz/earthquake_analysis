# Divide Turkey into 8 parts as shown below and sort them from safest to most dangerous 
# according the averages of the earthquake happened between dates 1900 -2016. 

from pyspark import SparkConf, SparkContext

sparkConf = SparkConf().setMaster("local[*]").setAppName("Spark 1")
sc = SparkContext(conf = sparkConf)

logger = sc._jvm.org.apache.log4j
logger.LogManager.getRootLogger().setLevel(logger.Level.ERROR)

def defineRegion(coordinates):
    latitude = coordinates[0]
    longitude = coordinates[1]

    if 39.0 <= latitude <= 42.0 and  26.0 <=longitude < 31.0:
        region = "region1"
    elif 39.0 <= latitude <= 42.0 and  31.0 <=longitude < 36.0:
        region = "region2"
    elif 39.0 <= latitude <= 42.0 and  36.0 <=longitude < 41.0:
        region = "region3"
    elif 39.0 < latitude <= 42.0 and  41.0 <=longitude < 45.0:
        region = "region4"
    elif 35.0 <= latitude < 39.0 and  26.0 <=longitude < 31.0:
        region = "region5"
    elif 35.0 <= latitude < 39.0 and  31.0 <=longitude < 36.0:
        region = "region6"
    elif 35.0 <= latitude < 39.0 and  36.0 <=longitude < 41.0:
        region = "region7"
    else:#35.0 <= latitude < 39.0 and  41.0 <=longitude < 45.0:
        region = "region8"   
        
    return  (region, 1)

def getCoordinates(earthquake):
    splitted = earthquake.split("\t")
    latitude = float(splitted[4])
    longitude = float(splitted[5])    
    return (latitude, longitude)
    
#Take earthquakes data and extract header
earthquakes = sc.textFile("19000101_20160507_3.5_9.0_54_950.txt")
headerR = earthquakes.first()
earthquakes = earthquakes.filter(lambda x: x!= headerR).map(getCoordinates).map(defineRegion)\
.reduceByKey(lambda x,y: x+y).map(lambda (x,y) : (y,x)).sortByKey()


print earthquakes.collect()
