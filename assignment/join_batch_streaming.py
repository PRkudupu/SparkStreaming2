from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import *

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
    transaction_details_schema=StructType([StructField('Customer_ID',StringType(),True),\
                                        StructField('Transaction_amount',StringType(),True),\
                                        StructField('Transaction_Rating',StringType(),True)])
    # read transaction details
    # join 2 data sets
    joinDF =customerDF.join(fileStreamDF,"Customer_ID")

    # query
    query= joinDF\
        .writeStream\
        .outputMode('append')\
        .format('console')\
        .start()\
        .awaitTermination()