""" TEMPORARY VIEW ADDED 
	SQL QUERY WRITTEN TO MANIPULATE THE DATA
"""
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
    
    
    # Read stream into a dataframe
    # Since the csv data includes a header row, we specify that here
    # We state the schema to use and the location of the csv files
	# maxFilesPerTrigger sets the number of new files to be considered in each trigger
    
    # Registering Table
    # Create a view which can later be queried like a table
    fileStreamDF.createOrReplaceTempView("LondonCrimeData")


    # Using SQL query
    # We use the LondonCrimeData view like a table
    # We only select the crime category and numConvictions columns
    # We narrow down the data to only include crimes committed in 2016
    
    # Create a trimmed version of the input dataframe with specific columns
    # We cannot sort a DataFrame unless aggregate is used, so no sorting here
   
	
    # We run in append mode, so only new rows are processed,
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








