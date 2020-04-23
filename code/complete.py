from pyspark.sql.types import *
from pyspark.sql import SparkSession


if __name__ == "__main__":

    # Set your local host to be the master node of your cluster
    # Set the appName for your Spark session
    # Join session for app if it exists, else create a new one
    sparkSession = SparkSession.builder.master("local")\
                              .appName("SparkStreamingAppendMode")\
                              .getOrCreate()


    # ERROR log level will generate fewer lines of output compared to INFO and DEBUG                          
    sparkSession.sparkContext.setLogLevel("ERROR")


    # InferSchema not yet available in spark structured streaming 
    # (it is available in static dataframes)
    # We explicity state the schema of the input data
    schema = StructType([StructField("lsoa_code", StringType(), True),\
                         StructField("borough", StringType(), True),\
                         StructField("major_category", StringType(), True),\
                         StructField("minor_category", StringType(), True),\
                         StructField("value", StringType(), True),\
                         StructField("year", StringType(), True),\
                         StructField("month", StringType(), True)])

    
    # Read stream into a dataframe
    # Since the csv data includes a header row, we specify that here
    # We state the schema to use and the location of the csv files
    fileStreamDF = sparkSession.readStream\
                               .option("header", "true")\
							   .option("maxFilePerTrigger",2)\
                               .schema(schema)\
                               .csv("../datasets/droplocation")


    # Use groupBy and agg functions to get total convictions per borough
    # The new column created will be called sum(value) - rename to something meaningfull
    # Order by number of convictions in descending order
    trimmedDF = fileStreamDF.groupBy("borough")\
							.agg("value":"sum")\
							.withColumnRenamed("sum(value)","convictions")\
		                    .orderBy("count",ascending=False)

	
    ##### Multiple Streaming aggregation is not supported   #### 
    # i.e. We already have performed an aggregation to get borough_convictions
    # A further aggregation such as the one below is not permitted 
    # data = borough_convictions.agg({"convictions":"sum"})
	
	
    # We run in complete mode, so only new rows are processed,
    # and existing rows in Result Table are not affected
    # The output is written to the console
    # We set truncate to false. If true, the output is truncated to 20 chars
    # Explicity state number of rows to display. Default is 20
    query = trimmedDF.writeStream\
                      .outputMode("complete")\
                      .format("console")\
                      .option("truncate", "false")\
                      .option("numRows", 30)\
                      .start()\
                      .awaitTermination()








