import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from affinn import Afinn


# Check that correct number of args have been passed as input
if __name__ == "__main__":

    if len(sys.argv) != 4:
        print("Usage: sentiment_analysis_streaming.py <hostname> <port>", file=sys.stderr)
        exit(-1)


    # Extract host and port from args    
    host = sys.argv[1]
    port = sys.argv[2]
    topic = sys.argv[3]

    
    # Set the app name when creating a Spark session
    # If a Spark session is already created for the app, use that. 
    # Else create a new session for that app
    spark = SparkSession\
        .builder\
        .appName("TwitterSentimentAnalysis")\
        .getOrCreate()


    # Set log level. Use ERRROR to reduce the amount of output seen
    spark.sparkContext.setLogLevel("ERROR")


    # use spark sessin to read from a kafka source
    # Read stream from kafka
    # Specify a bootstrap server a host on kafka cluster which spark can connect to
    # Specify the subscribe action which specifies which topic we need to subscribe
    # load the stream data
    tweetsDFRaw = spark.readStream\
                        .format("kafka")\
                        .option("kafka.bootstrap.servers", host + ":" + port)\
                        .option("subscribe", topic)\
                        .load()

    # cast the value as string and rename the column to tweet
    tweetsDF = tweetsDFRaw.selectExpr("CAST(value AS STRING) as tweet")
    
    # instantiate afin library
    afinn = Afinn()

    # function to add sentiment analysis score to every tweet
    def add_sentiment_score(text):
        sentiment_score = afinn.score(text)
        return sentiment_score
    
    # udf whcih would add sentiments for every tweet
    tweetsDF = tweetsDF.withColumn(
                                    "sentiment_score", 
                                    add_sentiment_score_udf(tweetsDF.tweet)
                                    )

    # new columnd would be added to tweets data frame
    tweetsDF = tweetsDF.withColumn(
                                    "sentiment_score", 
                                    add_sentiment_score_udf(tweetsDF.tweet)
                                    )

    # write the streams to console in append mode
    # we specify a trigger with processin time of 5 minutes
    query = tweetsDF.writeStream\
                                .outputMode("append")\
                                .format("console")\
                                .option("truncate", "false")\
                                .trigger(processingTime="5 seconds")\
                                .start()\
                                .awaitTermination()





