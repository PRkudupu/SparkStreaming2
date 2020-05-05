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
    personal_details_schema=StructType([StructField('Customer_ID',StringType(),True),\
                                        StructField('Gender',StringType(),True),\
                                        StructField('Age',StringType(),True)])

    # read customer details
    customerDF=sparkSession.read\
        .format("csv").option("header","true")\
            .schema(personal_details_schema)\
            .load("../datasets/customerDatasets/static_datasets/join_static_personal_details.csv")
    #transactional schema                                       
    transaction_details_schema=StructType([StructField('Customer_ID',StringType(),True),\
                                        StructField('Transaction_amount',StringType(),True),\
                                        StructField('Transaction_Rating',StringType(),True)])
    # read transaction details
    fileStreamDF =sparkSession.readStream\
                            .option("header","true")\
                            .option("maxFilesPerTrigger",1)\
                            .schema(transaction_details_schema)\
                            .csv("../datasets/customerDatasets/streaming_datasets/join_streaming_transaction_details")
    # join 2 data sets
    joinDF =customerDF.join(fileStreamDF,"Customer_ID")

    # query
    query= joinDF\
        .writeStream\
        .outputMode('append')\
        .format('console')\
        .start()\
        .awaitTermination()