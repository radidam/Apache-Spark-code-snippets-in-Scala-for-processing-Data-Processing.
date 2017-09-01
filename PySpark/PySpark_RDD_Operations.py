#%%
"""
--------------------------------------------------------------------
    Spark with Python: APPLY Project Solution

      @author: Ranjit
--------------------------------------------------------------------

"""
#%%
#Execute to create Spark sessions first 
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

#%%


#All data operation examples will use file - iris.csv and will build.

"""
-----------------------------------------------------------------------------
    Loading and Storing Data
    - Load that file into an RDD (Eg:SampleDataRDD) to Cache the RDD and count the number of lines

-----------------------------------------------------------------------------
"""
import os
Sample_Data_File = os.path.dirname(os.path.realpath('__file__'))+'/DataFiles/Sample-Data.csv'

#Create an RDD by loading from a file
SampleDataRDD = sc.textFile(Sample_Data_File)


SampleDataRDD.cache()
SampleDataRDD.count()
SampleDataRDD.take(50)

#%%
"""
Create a new RDD from SampleDataRDD with the following changes

    - In the Species column,first character should be capital
    - The numeric values should be rounded off (as integers)
"""

#Create a transformation function
def xformRow( RowStr) :
    
    if ( RowStr.find("Sepal") != -1):
        return RowStr
        
    attList=RowStr.split(",")
        
    attList[4] = attList[4].capitalize() 
    for i in range(0,4):
        attList[i] = str(round(float(attList[i])))
    
    return ",".join(attList)
    
xformedIris = SampleDataRDD.map(xformRow)
xformedIris.take(50)


#%%

"""
-----------------------------------------------------------------------------
Filter data(RDD) for lines that contain "Versicolor" or "Setosa"and count them.
-----------------------------------------------------------------------------
"""
FilteredData = xformedIris.filter(lambda x:  "Versicolor" in x or "Setosa" in x )
FilteredData.count()


#%%
"""
-----------------------------------------------------------------------------
Actions 
    - Find the average Sepal.Length in the SampleDataRDD
-----------------------------------------------------------------------------
"""

#Sepal.Length is a float value. So doing any integer operations 
#will not work. You need to use float functions

#function to check if a string has float value or not.
is_float = lambda x: x.replace('.','',1).isdigit() and "." in x

#Function to find the sum of all Sepal.Length values
def getSepalLength( Datarow) :
    
    if isinstance(Datarow, float) :
        return Datarow
        
    attList=Datarow.split(",")
    
    if is_float(attList[0]) :
        return float(attList[0])
    else:
        return 0.0

#Do a reduce to find the sum and then divide by no. of records.        
SepLenAvg=SampleDataRDD.reduce(lambda x,y : getSepalLength(x) + getSepalLength(y)) / (SampleDataRDD.count()-1)
    
print(SepLenAvg)

#%%
"""
-----------------------------------------------------------------------------
Key-Value RDD
    -Convert the SampleDataRDD into a key-value RDD with Species as key and Sepal.Length as the value.
    -Find the maximum of Sepal.Length by each Species.

-----------------------------------------------------------------------------
"""

#Create KV RDD
flowerData = SampleDataRDD.map( lambda x: ( (x.split(",")[4]), x.split(",")[0]))
flowerData.take(5)
flowerData.keys().collect()

#Remove header row from RDD
header = flowerData.first()
flowerKV= flowerData.filter(lambda line: line != header)
flowerKV.collect()

#find maximum of Sepal.Length by Species
maxData = flowerKV.reduceByKey(lambda x, y: max(float(x),float(y)))
maxData.collect()

#%%
"""
-----------------------------------------------------------------------------
Advanced Spark - Broadcast and Accumulator variables
    -Find the number of records in RDD, whose Sepal.Length is 
    greater than the Average Sepal Length we found in the earlier practice
-----------------------------------------------------------------------------
"""

#Initialize accumulator
sepalHighCount = sc.accumulator(0)

#Setup Broadcast variable
avgSepalLen = sc.broadcast(SepLenAvg)

#Write a function to do the compare and count
def findHighLen(line) :
    global sepalHighCount
    
    attList=line.split(",")
    
    if is_float(attList[0]) :
        
        if float(attList[0]) > avgSepalLen.value :
            sepalHighCount += 1
    return
    
#map for running the count. Also do a action to force execution of map
SampleDataRDD.map(findHighLen).count()

print(sepalHighCount)

