from __future__ import print_function
import logging
import time
from datetime import datetime



from nest_constants import *
from nest_similar_prop_spark import set_spark_session

from nest_similar_prop_spark import load_mongo_data


from pyspark.sql import SparkSession
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext,functions as F
from pyspark.sql.functions import col


# drop unecessary features (more than 50% records are null)

from nest_similar_prop_preprocessing import preprocessing
from nest_similar_prop_training import nest_similar_prop_training




def write_to_s3(df, path):

	
	#####################################################################################
	## Write JSON to S3/ Local
	#####################################################################################
	logging.warning("  saving to S3  -  ")
		
	try:
			'''
			df.coalesce(1) \
			  .write.format('json') \
			  .mode("overwrite") \
			  .save(path)
			'''
			df.repartition(1) \
			  .write.format('json') \
			  .mode("overwrite") \
			  .save(path)
			  
	except Exception as e:
		print("Error in write_to_s3() function  -  "+  str(e))
		raise e  		  




def write_to_elasticsearch(df):
	'''
	Future use 
	'''
	print("ES")


def write_to_mongo(df):
	'''
	Future use 
	'''
	
	print("Mongo")


#if __name__ == "__main__":

#def similar_properties(my_spark,sqlContext, ca_df_events, us_df_events):

def similar_properties(my_spark,sqlContext, df_events, mode):

	#https://realpython.com/python-logging/

	log_file = 'similar_properties_' + str(datetime.utcnow().strftime('%m_%d_%Y')) + '.log'

	logging.basicConfig(filename=log_file, filemode='a', format='%(asctime)s - %(message)s', datefmt='%d-%b-%y %H:%M:%S', level=logging.WARNING)
	
	# ORDER
	#logging.debug('DEBUG')
	#logging.info('INFO')
	#logging.warning('WARNING')
	#logging.exception("EXCEPTION")
	#logging.error("ERROR")
	

	
#	logging.warning('Admin logged out')
#	logging.exception("Exception occurred")
#	logging.error("error occurred")

	
	
	#df.registerTempTable("df_tbl")
	#sqlContext,my_spark = set_spark_session()
		
	l_obj = nest_similar_prop_training()  # get an instance of the class
	sqlContext.clearCache()

	

	############################
	## Run it for the CA
	############################
	
	if (mode == 'CA'):

		try:
			
			logging.warning(" CA RUN started -  ")
			ca_start_time = time.time()
			
			
			#pipeline1 = "{'$match': {'external_type':'list_hub', 'status':'active', 'country': 'US' }}"
			#ca_pipeline="[{'$match': {'status':'active', 'country': 'CA' }},{ $project : {'external_type':1,'beds':1,'bathrooms':1,'year_built':1,'images':1,'price_cents':1,'has_garage':1,'has_fireplace':1,'has_pool':1,'has_basement':1,'province':1,'property_sub_type':1,'parking_types':1,'postal':1,'address_street':1,'country':1,'city':1,'created_at':1,'utilities':1} } ]"
			ca_pipeline=P_CA_PIPELINE
				
			'''
			pipeline4 = [{'$match':{ 'created_at':{'$gte': {'$date': "2019-07-01T00:00:00Z" }}, 'status':'active', 'country': 'CA','external_type':'list_hub'
	                       }
	              }
	            ]

			'''



			ca_df = load_mongo_data(ca_pipeline,my_spark)


			
			
			################################
			# Caching the data in MEMORY
			################################
			
			#ca_df.registerTempTable("ca_df_tbl")
			#my_spark.catalog.cacheTable("ca_df_tbl")
			#ca_df = sqlContext.sql("select * from ca_df_tbl")
			
			# or its good idea to directly cache a dataframe instead of table like below
			ca_df.cache()


			
		
			#df_ca_filter=df.filter((df["country"]=='CA') & (df["status"]["symbol"] =='active'))
			
			
			#df_ca_filter = df.filter( (col("country")=='CA') & (col("status.symbol")=='active'))

			#df_ca_filter=df_ca_filter.filter(df_ca_filter["city"]=="Toronto")
				
			logging.warning("  preprocessing called -  ")
			ca_df_pre=preprocessing(ca_df,sqlContext)

			
			
			logging.warning("  transformation called -  ")
			ca_df_trans=l_obj.transformation(ca_df_pre,sqlContext)

				
			################################
			# Clear cache()
			################################
			#my_spark.catalog.uncacheTable("ca_df_tbl")
			###sqlContext.clearCache()

			logging.warning("  training called -  ")
			ca_df_training=l_obj.training(my_spark, ca_df_trans,df_events)
			
			
			################################
			# Caching the data in MEMORY
			################################
			#df2_ca.cache()

			
			logging.warning("  optimization called -  ")
			df_result=l_obj.optimization(ca_df_training,my_spark)
			#my_spark.catalog.uncacheTable("ca_df_tbl")

			'''
			
			##write_to_s3(df3_ca,P_CA_SIMILAR_PROP_S3_PATH)

			
			#df3_ca.registerTempTable("df3_tbl")
			#print("running show query now ")
			#sqlContext.sql("select * from df3_tbl where propertyid in ('5c1b0a92bc09e70001a4378f','5c818502be7c7e00011850bf','5c3eef2ca4fab100010eb651') ").show(10, False)
			'''
			
			print("CA took ", (time.time() -  ca_start_time))
			logging.warning(" CA Similarity Model Training took {} seconds.".format( time.time() -  ca_start_time))
		
		except Exception as e:
			logging.exception("EXCEPTION in MAIN  -  CA RUN ")
			raise e 	
	
	elif (mode == 'US'):		

		############################
		## Run it for the US
		############################
		

		try:
			
			logging.warning(" US RUN started -  ")
			us_start_time = time.time()
			
			#sqlContext.clearCache()
			#pipeline1 = "{'$match': {'external_type':'list_hub', 'status':'active', 'country': 'US' }}"
			
			#us_pipeline="[{'$match': {'status':'active', 'country': 'US' }},{ $project : {'external_type':1,'beds':1,'bathrooms':1,'year_built':1,'images':1,'price_cents':1,'has_garage':1,'has_fireplace':1,'has_pool':1,'has_basement':1,'province':1,'property_sub_type':1,'parking_types':1,'postal':1,'address_street':1,'country':1,'city':1,'created_at':1,'utilities':1} } ]"

			#us_pipeline=   "[{'$match': {'created_at':{'$gte': {'$date': '2019-07-01T00:00:00Z' }}, 'status':'active', 'country': 'CA' }},{ $project : {'external_type':1,'beds':1,'bathrooms':1,'year_built':1,'images':1,'price_cents':1,'has_garage':1,'has_fireplace':1,'has_pool':1,'has_basement':1,'province':1,'property_sub_type':1,'parking_types':1,'postal':1,'address_street':1,'country':1,'city':1,'created_at':1,'utilities':1} } ]"

			
			#us_pipeline=   "[{'$match': {'created_at':{'$gte': {'$date': '2019-07-01T00:00:00Z' }}, 'status':'active', 'country': 'US' }},{ $project : {'external_type':1,'beds':1,'bathrooms':1,'year_built':1,'images':1,'price_cents':1,'has_garage':1,'has_fireplace':1,'has_pool':1,'has_basement':1,'province':1,'property_sub_type':1,'parking_types':1,'postal':1,'address_street':1,'country':1,'city':1,'created_at':1,'utilities':1} } ]"
			us_pipeline=P_US_PIPELINE

			us_df = load_mongo_data(us_pipeline,my_spark)


			
			
			################################
			# Caching the data in MEMORY
			################################
			
			#ca_df.registerTempTable("ca_df_tbl")
			#my_spark.catalog.cacheTable("ca_df_tbl")
			
			# or its good idea to directly cache a dataframe instead of table like below
			us_df.cache()


			#df_ca_filter = sqlContext.sql("select * from ca_df_tbl")
			
		
			#df_ca_filter=df.filter((df["country"]=='CA') & (df["status"]["symbol"] =='active'))
			
			
			#df_ca_filter = df.filter( (col("country")=='CA') & (col("status.symbol")=='active'))

			#df_ca_filter=df_ca_filter.filter(df_ca_filter["city"]=="Toronto")
				
			logging.warning("  preprocessing called -  ")
			us_df_pre=preprocessing(us_df,sqlContext)

			
			
			logging.warning("  transformation called -  ")
			us_df_trans=l_obj.transformation(us_df_pre,sqlContext)

				
			################################
			# Clear cache()
			################################
			#my_spark.catalog.uncacheTable("ca_df_tbl")
			#sqlContext.clearCache()

			logging.warning("  training called -  ")
			us_df_training=l_obj.training(my_spark, us_df_trans,df_events)
			


			
			################################
			# Caching the data in MEMORY
			################################
			#df2_ca.cache()

			
			logging.warning("  optimization called -  ")
			df_result=l_obj.optimization(us_df_training,sqlContext)
			#my_spark.catalog.uncacheTable("ca_df_tbl")

			
			
			##write_to_s3(df3_ca,P_CA_SIMILAR_PROP_S3_PATH)

			
			#df3_ca.registerTempTable("df3_tbl")
			#print("running show query now ")
			#sqlContext.sql("select * from df3_tbl where propertyid in ('5c1b0a92bc09e70001a4378f','5c818502be7c7e00011850bf','5c3eef2ca4fab100010eb651') ").show(10, False)
			
			
			print("US took ", (time.time() -  us_start_time))
			logging.warning(" US Similarity Model Training took {} seconds.".format( time.time() -  us_start_time))
		
		except Exception as e:
			logging.exception("EXCEPTION in MAIN  -  US RUN ")
			raise e 	
		

	return df_result
	