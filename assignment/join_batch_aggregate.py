from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import *
from pyspark.sql.functions import udf

if __name__ == "__main__":
    #Instantiate a new spark session
    sparkSession = SparkSession\
        .builder\
        .appName("join")\
        .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")
    
    #define the schema
    # read customer details
    #transactional schema                                       
    # read transaction details
    # join 2 data sets
    
    # group by geneder and aggregate transaction amount
    
    # round function
    
    #define udf
    #add average transaction amount
    #query
    query= spending_per_gender\
        .writeStream\
        .outputMode('complete')\
        .format('console')\
        .start()\
        .awaitTermination()