
# DEFINE ALL CONSTANTS HERE

from pyspark.sql.types import *

P_INPUT_SPARK_HOST_NAME="mongodb://127.0.0.1/backend_production.properties"

#P_INPUT_HOST_NAME = "mongodb://3.210.155.32/backend_production.properties"

P_OUTPUT_SPARK_HOST_NAME="mongodb://127.0.0.1/backend_production.properties"

P_APP_NAME="similar_properties"

P_MASTER="local[2]"

P_DRIVER_MEMORY="15g"

P_EXECUTOR_MEMORY="6g"

P_EXECUTOR_CORES=2

P_EXECUTOR_INSTANCES=1

P_PIVOT_MAX_VALUES=100000

P_MONGO_SPARK_CONNECTOR="org.mongodb.spark:mongo-spark-connector_2.11:2.4.0"

P_DEFAULT_SOURCE="com.mongodb.spark.sql.DefaultSource"

P_DB="backend_production"

P_COLLECTION="properties"

P_DEBUG="logging.DEBUG"
         
#P_SIMILAR_PROP_S3_PATH="/Users/jatinmalhotra/Desktop/NestReady_Recommendation_System/CODING/nest_recommendation_engine/similar_properties"
P_CA_SIMILAR_PROP_S3_PATH="s3://aws-logs-966730287427-us-east-1/ca_similar_property_json"
P_US_SIMILAR_PROP_S3_PATH="s3://aws-logs-966730287427-us-east-1/us_similar_property_json"


P_SCHEMA=StructType(  (
                StructField('_id',StringType(),True),
                StructField('address_street',StringType(),True),
                StructField('bathrooms',IntegerType(),True),
                StructField('beds',IntegerType(),True),
                StructField('city',StringType(),True),
                StructField('country',StringType(),True),
                StructField('created_at',TimestampType(),True),
                StructField('external_type',StringType(),True),
                StructField('has_basement',BooleanType(),True),
                StructField('has_fireplace',BooleanType(),True),
                StructField('has_garage',BooleanType(),True),
                StructField('has_pool',BooleanType(),True),
                StructField('images',ArrayType(StructType((StructField('_id',StringType(),True),StructField('sha1',StringType(),True))),True),True),
                StructField('parking_types',StringType(),True),
                StructField('postal',StringType(),True),
                StructField('price_cents',IntegerType(),True),
                StructField('property_sub_type',StringType(),True),
                StructField('utilities',StringType(),True),
                StructField('province',StringType(),True),
                StructField('year_built',IntegerType(),True),
                
               )
             
            )

P_INPUT_MONGO_HOST='localhost'

P_INPUT_MONGO_PORT=27017

P_EVENT_LOGS_A='/Users/jatinmalhotra/Desktop/nsegmentlogs-v3/*/'
P_EVENT_LOGS_NA='/Users/jatinmalhotra/Desktop/nsegmentlogs-v3-na/*/'

P_TOP_N=5

P_MODEL_THRESHOLD=0.5

P_CA_PIPELINE="[{'$match': {'status':'active', 'country': 'CA' }},{ $project : {'external_type':1,'beds':1,'bathrooms':1,'year_built':1,'images':1,'price_cents':1,'has_garage':1,'has_fireplace':1,'has_pool':1,'has_basement':1,'province':1,'property_sub_type':1,'parking_types':1,'postal':1,'address_street':1,'country':1,'city':1,'created_at':1,'utilities':1} } ]"

P_US_PIPELINE=  "[{'$match': {'created_at':{'$gte': {'$date': '2019-07-01T00:00:00Z' }}, 'status':'active', 'country': 'US' }},{ $project : {'external_type':1,'beds':1,'bathrooms':1,'year_built':1,'images':1,'price_cents':1,'has_garage':1,'has_fireplace':1,'has_pool':1,'has_basement':1,'province':1,'property_sub_type':1,'parking_types':1,'postal':1,'address_street':1,'country':1,'city':1,'created_at':1,'utilities':1} } ]"

P_SCORE_COUNT= "db.properties.find( { score: { $gte: 0 } } ).count()"
