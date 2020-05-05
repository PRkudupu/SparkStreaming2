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

    #Read the stream by specifying the host name ans port
    lines = spark\
        .readStream\
        .format("socket")\
        .option("host", host)\
        .option("port", port)\
        .load()
	
	#split based on space 
    words = lines.select(
        explode(
            split(lines.value, " ")
        ).alias("word")
    )
    #UDF. This function finds the word which starts with #
    def extract_tags(word):
        if word.lower().startswith("#"):
            return word
        else:
            return "nonTag"

    extract_tags_udf = udf(extract_tags, StringType())
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
















































