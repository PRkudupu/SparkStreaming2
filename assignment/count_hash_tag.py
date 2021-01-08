import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import udf
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

    #Read streaming data from the socket by specifying the host name ans port
   
	#split based on space 
    
    #UDF. This function finds the word which starts with #
 
	#Add new column tags w
    resultDF = words.withColumn("tags", extract_tags_udf(words.word))

    """Filter non tagGroup 
    group by tag and do a count on tags """
    hashtagCounts = resultDF.where(resultDF.tags != "nonTag")\
                          .groupBy("tags")\
                          .count()\
                          .orderBy("count", ascending=False)

    query = hashtagCounts.writeStream\
                      .outputMode("complete")\
                      .format("console")\
                      .option("truncate", "false")\
                      .start()\
                      .awaitTermination()
















































