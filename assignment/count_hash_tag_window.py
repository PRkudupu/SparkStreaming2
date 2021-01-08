import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import udf
from pyspark.sql.functions import window
from pyspark.sql.types import *

if __name__ == "__main__":

    if len(sys.argv) != 3:
        print("Usage: spark-submit m02_demo09_countHashtags.py <hostname> <port>", 
                file=sys.stderr)
        exit(-1)

    host = sys.argv[1]
    port = int(sys.argv[2])
    #Instantiate a new spark session
    spark = SparkSession\
        .builder\
        .appName("HashtagCount")\
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    #Read the stream by specifying the host name ans port
    #Include timestammp.We use the twitter event stamp 
   
	#split based on space 
    words = lines.select(
        explode(
            split(lines.value, " "))\
        .alias("word"),
        lines.timestamp
    )
    #UDF. This function finds the word which starts with #
  
	#Add new column tags w
    resultDF = words.withColumn("tags", extract_tags_udf(words.word))

    """Filter non tagGroup 
       group by window  
       Window size 50 seconds 
       sliding interval is 30 seconds
       Within the window group by tag.
       Count the frequency of tag
       Oder in descending order."""
    
    query = hashtagCounts.writeStream\
                      .outputMode("complete")\
                      .format("console")\
                      .option("truncate", "false")\
                      .start()\
                      .awaitTermination()
















































