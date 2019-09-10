#!/usr/bin/env python
# coding: utf-8


#####################################################################################
# Import Libraries
#####################################################################################
from collections import defaultdict
import json
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext,functions as F
from pyspark.sql.functions import col
import time
from nest_constants import *
from nest_similar_prop_initiate import *





#####################################################################################
# Convert user events time into epochs
#####################################################################################
def get_epochs(val):
    return int(time.mktime(time.strptime(val, '%Y%m%d%H%M%S')))




#####################################################################################
# Set Spark session
#####################################################################################
'''
def set_spark_session():
    try:
        spark = SparkSession \
                .builder \
                .appName(P_APP_NAME) \
                .config('spark.driver.memory', P_DRIVER_MEMORY) \
                .config('spark.executor.memory', P_EXECUTOR_MEMORY) \
                .config('spark.executor.cores',P_EXECUTOR_CORES) \
                .config('spark.mongodb.input.uri', P_INPUT_SPARK_HOST_NAME) \
                .config('spark.mongodb.output.uri', P_OUTPUT_SPARK_HOST_NAME) \
                .config('spark.sql.pivotMaxValues', P_PIVOT_MAX_VALUES) \
                .config('spark.jars.packages', P_MONGO_SPARK_CONNECTOR) \
                .config('spark.sql.inMemoryColumnarStorage.compressed',True) \
                .config('spark.sql.broadcastTimeout',"36000") \
                .getOrCreate()
 
    except Exception as e:
        #print("Error in SparkSession  -  "+  str(e))
        logging.exception("EXCEPTION -  Setting SparkSession")
        raise e    
        
    # Set SQL Context
    sqlContext=SQLContext(spark)

    return sqlContext,spark
'''

#####################################################################################
# load user behavoral data from S3 bucket
#####################################################################################
def load_user_event_logs(spark):
    
    
    #Anonymous logs
    df_a = spark.read.format("json").load(P_EVENT_LOGS_A).filter( col("propertyId").isNotNull() & 
                                                             col("userId").isNotNull()
                                                    )     
    #Non-Anonymous logs
    df_na = spark.read.format("json").load(P_EVENT_LOGS_NA).filter( col("propertyId").isNotNull() & 
                                                             col("userId").isNotNull()
                                                    )    
    
    
    #merge both datasets
    df_a.registerTempTable("df_tbl")
    df_na.registerTempTable("df_na_tbl")
    
    df_union=spark.sql('''
    select userId as USER_ID, propertyId as ITEM_ID, replace(propertyCountry,'Canada','CA') as COUNTRY, get_epochs(eventTimestamp) as TIMESTAMP from df_tbl
    UNION ALL
    select NVL(NVL(contactId.Id, userId),1) as USER_ID, propertyId as ITEM_ID, replace(propertyCountry,'Canada','CA') as COUNTRY, get_epochs(eventTimestamp) as TIMESTAMP from df_na_tbl
    ''')
    return df_union

   


# In[3]:


#####################################################################################
# Use only active properties from user behavoral data
# connect to mongodb and check if property is active or not
#####################################################################################
def filter_inactive_properties(df):
       
       from pymongo import MongoClient
       from bson.objectid import ObjectId
       import pandas as pd
       
       #g_input="mongodb://10.0.1.70"
       #g_input="mongodb://3.210.155.32"
       
       
       
       client = MongoClient(P_INPUT_MONGO_HOST,P_INPUT_MONGO_PORT)
       
       #client = MongoClient('mongodb://localhost:27017')


       db = client.backend_production

       df_pd = pd.DataFrame(columns=['propertyId'])
       
       l_cnt=0
       
       for i in df.select('item_id').distinct().collect():
          #cnt= db.properties.find( {'mls_number': 'W4505836', 'status': 'active'}).count()
          #cnt= db.properties.find( {'_id': ObjectId(i[0]), 'status': 'active'}).count()
          #cnt= db.properties.count_documents({'_id': ObjectId('598115d9d7d1fe6eb6efd684')})

          cnt= db.properties.count_documents({'_id': ObjectId(i[0])})
           
          if cnt == 0:
               l_cnt=l_cnt+1
               df_pd.loc[l_cnt] = [i[0]]
       
       #print(df_pd.shape)
       spark_df = sqlContext.createDataFrame(df_pd) 
       
       # Create tables
       spark_df.registerTempTable("spark_df_tbl")
       df.registerTempTable("df_tbl")
       
       df_active=spark.sql('''
                           select * from df_tbl a where 1=1 
                           and not exists 
                                   (select 1 from spark_df_tbl b 
                                    where a.item_id=b.propertyId)

                           ''')
       df_filter_active=df_active.select('user_id', 'item_id', 'country', 'timestamp').distinct()

       
       return df_filter_active 
 


# In[4]:



#####################################################################################
# setup properties popularity calculations to be used in popularity model
#####################################################################################
def events_popularity_score(df):
      
      df.registerTempTable("df_tbl")
      
      df_popularity=spark.sql('''
                      select item_id,country, count(distinct USER_ID) as count_score
                      from df_tbl group by 1,2 order by 3 desc
                      ''')
      
      '''
      df_popularity=df_popularity.coalesce(1)
      from pyspark.sql.functions import monotonically_increasing_id
      df_popularity=df_popularity.withColumn('score', monotonically_increasing_id())
      '''
      
      from pyspark.sql import Window
      import pyspark.sql.functions as psf

      cnt_score = Window.orderBy(psf.desc("count_score"))

      df_popularity_sc = df_popularity.withColumn("score", 
                          psf.dense_rank().over(cnt_score)
                          )
      
      return df_popularity_sc


#####################################################################################
# update property popularity score in mongodb
#####################################################################################
def update_score_in_mongo(df):
    
    #df=df.select('_id','score')
    
    import pyspark.sql.functions as sfunc
    from pyspark.sql.types import StructType
    

   
    udf_struct_id = sfunc.udf(
        lambda x: tuple((str(x),)), 
        StructType([StructField("oid",  StringType(), True)])
    )

        
    df = df.withColumn('_id', udf_struct_id('_id'))
    
    print("Numer of properties to update in mongo : ",df.count())

    start_time = time.time()

    
    '''
    df.write.format("com.mongodb.spark.sql.DefaultSource")\
            .mode("append") \
            .option("database",P_DB)\
            .option("collection", P_COLLECTION)\
            .option("replaceDocument", "false")\
            .save()
    '''

    df.repartition(1) \
        .write.format("json") \
        .mode("overwrite") \
        .option("header","true")\
        .save("popularity")

    #Above will always append new fields to the existing records and will not change/touch existing fields
    #use this to update score in mongo for popular matrix

    
    print("Mongo score updates took - ", (time.time() -  start_time) )


#####################################################################################
# Set final popularity score using property count in user events and property similarity score
#####################################################################################
def find_event_items_similarities(df, df_ca):
    df.registerTempTable("df_tbl")
    df_ca.registerTempTable("df_ca_tbl")
    
    df_r=spark.sql('''
        select df_ca.id1 as _id, min((df.score + df_ca.score)) as score
        from df_tbl df, 
             df_ca_tbl df_ca
        where df.item_id=  df_ca.id2
        group by 1
    ''')
    
    #df_r.cache()
    
    return df_r
    
    


# In[7]:


#####################################################################################
# Incase popularity score needs tobe normalized for ex- in the range of (0,1)
#####################################################################################
def normalize_score(df):
    
    from pyspark.ml.linalg import Vectors
    from pyspark.ml.feature import VectorAssembler
    from pyspark.ml.feature import StandardScaler


    assembler = VectorAssembler(
        inputCols=["score"],
        outputCol="score_v")

    output = assembler.transform(df)
    
    # Normalize each Vector using $L^1$ norm.
    
    scaler = StandardScaler(inputCol="score_v", outputCol="popularity_score",
                        withStd=False, withMean=True)

    # Compute summary statistics by fitting the StandardScaler
    scalerModel = scaler.fit(output)

    # Normalize each feature to have unit standard deviation.
    scaledData = scalerModel.transform(output)
    
    return scaledData


# In[7]:


#def get_similar_properties(df_similar_items, item_id):
 #   return df_similar_items.filter(col('_id')==item_id )


    
#####################################################################################
# user properties popularity score
#####################################################################################
def user_item_popularity(df):
    
    from pyspark.sql.window import Window
    from pyspark.sql.functions import rank, col
    
    df.registerTempTable("df_tbl")
    
    df_tmp=spark.sql('''
                select user_id, item_id, count(*) as count_score from df_tbl
                group by 1, 2 order by 1,3 desc
                ''')
    

    window = Window.partitionBy(df_tmp['user_id']).orderBy(df_tmp['count_score'].desc())

    return df_tmp.select('user_id','item_id', rank().over(window).alias('score'))


#####################################################################################
# user properties similarities
#####################################################################################
def user_item_similarities(df, df_similar):
    
    df.registerTempTable("df_user_events")
    df_similar.registerTempTable("df_similar_items")
    
    return spark.sql('''
        select df.user_id, df_similar.id1 as _id, min((df.score + df_similar.similarity_score)) as score
        from df_user_events df, 
             df_similar_items df_similar
        where df.item_id=  df_similar.id2
        group by 1,2 order by 1,3 asc
    ''')



def popularity_based_model(df):

  df.registerTempTable("df_tbl")

  df_mongo=spark.sql('''
                    select id1 as _id, min(similarity_score + popularity_score) as score
                    from df_tbl
                    group by 1
                    ''')

  update_score_in_mongo(df_mongo)



def user_personalization(df_similarity,df_active):

    df_pop=user_item_popularity(df_active)

    df_tmp=user_item_similarities(df_pop,df_similarity)



    df_save=df_tmp.select("user_id","_id","score")\
    .withColumn("Recommendations", F.struct(F.col("_id"), F.col("score")))\
    .select("user_id","Recommendations")\
    .groupby("user_id").agg(F.collect_list("Recommendations").alias("Recommendations"))  
    
    
    start_time = time.time()


    df_save.repartition(1) \
        .write.format("json") \
        .mode("overwrite") \
        .option("header","true")\
        .save("userrr_2")
    
    print("Saving user personalization json took - ", (time.time() -  start_time) )




if __name__ == "__main__":

  print("Main Started -")


  
  # set spark session
  sqlContext,spark=set_spark_session()
  

  # register for unix epochs
  spark.udf.register("get_epochs", get_epochs)     

  df_union=load_user_event_logs(spark)
  #print("Total number of user events", df_union.count())

  
  df_active=filter_inactive_properties(df_union)
  #print("Total number of user events having active items", df_active.count())

    
  df=events_popularity_score(df_active)
  print("user events processing done")



    
  ca_data= df.filter(col('COUNTRY')=='CA')
  us_data= df.filter(col('COUNTRY')=='US')


  df_ca_similar_items = similar_properties(spark, sqlContext, ca_data, 'CA')
  df_us_similar_items = similar_properties(spark, sqlContext, us_data, 'US')
  print("similar properties model completed")

  
  df_ca_similar_items.cache()
  df_us_similar_items.cache()


  popularity_based_model(df_ca_similar_items)
  popularity_based_model(df_us_similar_items)
  print("popularity model completed")

  user_personalization(df_ca_similar_items,df_active)
  user_personalization(df_us_similar_items,df_active)
  print("user personalization model completed")

  


