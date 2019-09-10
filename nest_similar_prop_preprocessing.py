from pyspark.sql import SQLContext,functions as F
import sys
import logging


def get_images(df):


	try:
		tmp_images = df.withColumn("images", F.explode("images")) \
		                	.groupBy("_id").count()

		df_images=df.join(tmp_images, '_id','left') \
		            		.withColumnRenamed("count", "images_count") \
		            		.drop("images")

	except Exception as e:
		#print("Error in get_images() function  -  "+  str(e))
		logging.exception("EXCEPTION -  get_images() function")
		raise e   
    
	return df_images  		
	



def get_utilities(df):

	#counting total no of utilities			
	try:

		df_ca_f2 = df.withColumn("utilities",F.split(F.regexp_replace("utilities", r"(^\[)|(\]$)|(')", ""), ", "))
		df1 = df_ca_f2.withColumn("utilities_array",F.explode("utilities")).groupBy("_id").count()
		df_utilities=df_ca_f2.join(df1, '_id','outer').withColumnRenamed("count", "utilities_count")
	except Exception as e:
		#print("Error in get_utilities() function  -  "+  str(e))
		logging.exception("EXCEPTION -  get_utilities() function")
		raise e   
    
	return df_utilities  		
	    		


def preprocessing(df,sqlContext):


	try:
		
		#####################################################################################
		# Feature Engineer Images 
		#####################################################################################
		#df_sub_f1 = df

		df_sub_f2 = get_images(df)


		df_sub_f3 = get_utilities(df_sub_f2)
				
		df_sub_f3.registerTempTable("df_sub_f3_tbl")


		'''
		mean_values=sqlContext.sql(
	                select ceil(mean(beds)),
	                       ceil(mean(bathrooms)),
	                       round(mean(price_cents))
	                from df_sub_f3_tbl
	               ).collect()

		
		df1=sqlContext.sql(
	                 select _id.*,
	                        created_at,
	                        updated_at,
	                        CAST (NVL(year_built,'UNKNOWN') as VARCHAR(500)) as year_built1,
	                        NVL(images_count,0) as images_count,
	                        nvl(beds,{0}) as beds ,
	                        nvl(bathrooms,{1}) as bathrooms,
	                        CAST ((nvl(price_cents,{2})) as INTEGER) as price_cents,
	                        nvl(city, 'UNKNOWN') as city,
	                        (CASE has_garage
	                           WHEN TRUE THEN 1
	                           ELSE 2
	                        END) as has_garage,
	                        (CASE has_fireplace
	                           WHEN TRUE THEN 1
	                           ELSE 2
	                        END) as has_fireplace,
	                        (CASE has_pool
	                           WHEN TRUE THEN 1
	                           ELSE 2
	                        END) as has_pool,
	                        (CASE has_basement
	                           WHEN TRUE THEN 1
	                           ELSE 2
	                        END) as has_basement,
	                         NVL(trim(province), 'UNKNOWN') province,
	                        CASE WHEN (replace(trim(property_sub_type),' ',''))='' THEN 'UNKNOWN' ELSE nvl(replace(trim(property_sub_type),' ',''),'UNKNOWN') END property_sub_type,
	                        NVL(CAST (parking_types as VARCHAR(500)),'UNKNOWN') parking_types,
	                        CASE WHEN (replace(trim(postal),' ',''))='' THEN 'UNKNOWN' ELSE nvl(replace(trim(postal),' ',''),'UNKNOWN') END postal,
	                        CAST (NVL(utilities_count, 0) as INTEGER) as utilities_count,
	                        utilities,
	                        CASE WHEN (replace(trim(address_street),' ',''))='' THEN 'UNKNOWN' ELSE nvl(replace(trim(address_street),' ',''),'UNKNOWN') END address_street
                   
	                        
	                from df_sub_f3_tbl
	                \
	               .format(\
	                       mean_values[0][0],\
	                       mean_values[0][1],\
	                       mean_values[0][2]),\
	              )
		'''

		

		df1=sqlContext.sql('''
                 select _id,
                 		created_at,
                        NVL(year_built,0) as year_built,
                        CAST( NVL(images_count,0) as INTEGER) as images_count,
                        nvl(beds,0) as beds ,
                        nvl(bathrooms,0) as bathrooms,
                        nvl(price_cents,0) as price_cents,
                        nvl(city,'UNKNOWN') as city,
                        (CASE has_garage
                           WHEN TRUE THEN 1
                           ELSE 2
                        END) as has_garage,
                        (CASE has_fireplace
                           WHEN TRUE THEN 1
                           ELSE 2
                        END) as has_fireplace,
                        (CASE has_pool
                           WHEN TRUE THEN 1
                           ELSE 2
                        END) as has_pool,
                        (CASE has_basement
                           WHEN TRUE THEN 1
                           ELSE 2
                        END) as has_basement,
                        nvl(province,'UNKNOWN') as province,
                        NVL(trim(property_sub_type), 'UNKNOWN') property_sub_type,
                        NVL(parking_types,'UNKNOWN') as parking_types,
                        CASE WHEN (replace(trim(postal),' ',''))='' THEN 'UNKNOWN' ELSE nvl(replace(trim(postal),' ',''),'UNKNOWN') END postal,
                        CAST (nvl(utilities_count,0) as INTEGER) as utilities_count,
                        CASE WHEN (replace(trim(address_street),' ',''))='' THEN 'UNKNOWN' ELSE nvl(replace(trim(address_street),' ',''),'UNKNOWN') END address_street1
                from df_sub_f3_tbl
                ''')

		
		return df1
	except Exception as e:
		#print("Exception raised in preprocessing() function : "+ str(e) )
		logging.exception("EXCEPTION -  preprocessing() function")
		raise e
		
