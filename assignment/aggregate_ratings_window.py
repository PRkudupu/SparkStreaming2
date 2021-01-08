from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import *
from pyspark.sql.functions import udf
import time
import datetime
from pyspark.sql.functions import window

if __name__ == "__main__":
    #Instantiate a new spark session
    sparkSession = SparkSession\
        .builder\
        .appName("join")\
        .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")
    
    #define the schema
    personal_details_schema=StructType([StructField('Customer_ID',StringType(),True),\
                                        StructField('Gender',StringType(),True),\
                                        StructField('Age',StringType(),True)])

    # read customer details
    #transactional schema                                       
    # read transaction details
    # join 2 data sets
     
    # add time stamp
    def add_timestamp():
        ts=time.time()
        timestamp=datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        return timestamp
    
    #create udf function for timestamp
    add_timestamp_udf=udf(add_timestamp,StringType())
    
    #add time stamp columns
    df_with_timestamp=joinDF.withColumn("timestamp",add_timestamp_udf())

    #split customers by age groups
    def add_age_group(age):
        if int(age) <25: return 'less than 25'
        elif int(age) <40: return '25 to 40'
        else: return 'more than 40'
    
    #create UDF
    
    # group by geneder and aggregate transaction amount
   
    # aggregate the age group
    
    # add window functions
    
    # round function
    def round_func(amount):
        return ("%.2f"% amount)
    
    #define udf
   
    #add average transaction amount
    windowed_transactions=windowed_transactions.withColumn(
                                                       "Average_Transaction_Amount",
                                                        round_udf("avg(Transaction_Amount)")
                                                    )\
                                            .drop("avg(Transaction_Amount)")
    #query
    query= windowed_transactions\
        .writeStream\
        .outputMode('complete')\
        .format('console')\
        .start()\
        .awaitTermination()