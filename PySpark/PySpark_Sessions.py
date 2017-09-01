#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
--------------------------------------------------------------------
    Spark with Python: APPLY Project Solution

      @author: Ranjit
--------------------------------------------------------------------

"""
#%%
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)


#%%
#Initialize SparkSession and SparkContext
from pyspark.sql import SparkSession
#from pyspark.sql import SQLContext
#from pyspark import SparkContext,SparkConf

 
'''
conf = (SparkConf().setAppName('Application name: First App'))
#        .setAppName("CountingSheep")
conf.set('spark.hadoop.avro.mapred.ignore.inputs.without.extension', 'false')
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)
sql = SQLContext(sc)



df = (sql.read
#         .format("com.databricks.spark.csv")
         .option("header", "true")
         .load("/Users/s101531/src.csv"))
df.collect()

'''


#Create a Spark Session
Session = SparkSession \
    .builder \
    .master("local[2]") \
    .appName("Application name: First App") \
    .config("spark.executor.memory", "1g") \
    .config("spark.cores.max","2") \
    .getOrCreate()
    
#Get the Spark Context (sc)from Spark Session    
sc = Session.sparkContext

#Test Spark
testData = sc.parallelize([3,6,4,2])
testData.count()
#check http://localhost:4040 to see if Spark is running

#%%
import os
#Specify file name and the path
file = os.path.dirname(os.path.realpath('__file__'))+'/DataFiles/movie-tweets.csv'

#Create an RDD by loading from a file

tweetsRDD = sc.textFile(file)
#tweetsRDD = sc.textFile(os.path.dirname(os.path.realpath('__file__'))+'/DataFiles/movietweets.csv')

#Check if the file is read correctly - show top 5 records
tweetsRDD.take(5)
#%%
#Transform Data Example - change to upper Case
ucRDD = tweetsRDD.map( lambda x : x.upper() )
ucRDD.take(5)
#%%
#Action - Count the number of tweets
tweetsRDD.count()

