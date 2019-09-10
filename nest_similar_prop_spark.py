import pandas as pd
from collections import defaultdict
from pyspark.sql import SparkSession
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext,functions as F
from pyspark.sql.types import *
import logging
from nest_constants import *


def set_spark_session():

	try:

		my_spark = SparkSession \
		     .builder \
		     .appName(P_APP_NAME) \
		     .config('spark.driver.memory', P_DRIVER_MEMORY) \
		     .config('spark.executor.memory', P_EXECUTOR_MEMORY) \
		     .config("spark.executor.cores",P_EXECUTOR_CORES) \
     		 .config("spark.mongodb.input.uri", P_INPUT_SPARK_HOST_NAME) \
     		 .config("spark.mongodb.output.uri", P_OUTPUT_SPARK_HOST_NAME) \
     		 .config("spark.sql.pivotMaxValues", P_PIVOT_MAX_VALUES) \
		     .config('spark.jars.packages', P_MONGO_SPARK_CONNECTOR) \
		     .config('spark.executor.heartbeatInterval","7200s') \
		     .config('spark.sql.inMemoryColumnarStorage.compressed',True) \
             .config('spark.sql.broadcastTimeout',"36000") \
		     .getOrCreate()
 
 
					#spark.network.timeout=420000
					#spark.dynamicAllocation.executorIdleTimeout 600s

	except Exception as e:
		#print("Error in SparkSession  -  "+  str(e))
		logging.exception("EXCEPTION -  Setting SparkSession")
		raise e    


	
	# Set SQL Context
	sqlContext=SQLContext(my_spark)

	return sqlContext,my_spark


def load_mongo_data(P_PIPELINE,my_spark):


	# Read data from Mongo
	df = my_spark.read.format(P_DEFAULT_SOURCE)\
		    .option("pipeline", P_PIPELINE)\
		    .option("inferSchema","false")\
		    .schema(P_SCHEMA)\
		    .load()
	

	#df=df.limit(1000)
	

	'''
	df = my_spark.read.format(P_DEFAULT_SOURCE)\
		    .option("database",P_DB)\
		    .option("collection", P_COLLECTION)\
		    .option("pipeline", P_PIPELINE)\
		    .option("inferSchema","false")\
		    .load()
	'''
	#df = sqlContext.read.load(format="com.mongodb.spark.sql.DefaultSource",pipeline=P_PIPELINE) 
	  

	return df



def alterschema(df):

	
	for i in df.columns:
		if i not in ('parking_types','images', 'status'):
			df= df.withColumn(i, df[i].cast(StringType()))

	df_casted = my_spark.read.format(P_DEFAULT_SOURCE)\
						    .option("database",P_DB)\
						    .option("collection", P_COLLECTION)\
						    .schema(df.schema)\
						    .load()

	#####################################################################################
    # Reload property data from mongodb by explicitly specifying  Schema as String
    #####################################################################################
    	

	    	
    
    
	for i in df.columns:
	    #df= df.withColumn(i, df[i].cast(StringType()))
	    #print(df.schema[i].dataType)
	    b=df.schema['stories'].dataType
	    
	    if ((df.schema[i].dataType == df.schema['stories'].dataType) | (df.schema[i].dataType == df.schema['total_sqft'].dataType)\
	         | (df.schema[i].dataType == df.schema['basement'].dataType) | (df.schema[i].dataType == df.schema['brokerage'].dataType)
	         | (df.schema[i].dataType == df.schema['external_type'].dataType) 
	       ):
	        print(df.schema[i].dataType)
	        df= df.withColumn(i, df[i].cast(StringType()))
	        
						    

	return df	