###########################
# VALIDATE NET CAT UTILITY AND SPARK
#########################
Start netcat utility
nc -L -p 9999
nc localhost 9999


###########################
# WORD COUNT PROGRAM
#########################
spark-submit streaming.py localhost 9999

# Input sentence for word count
Baked chicken is healthier than fried chicken
I prefer fried fish over fried chicken

cd C:\Users\tekstudent\Desktop\tutorials\code

############################
#DIFFERENT TYPES OF OUTPUT MODES
############################
spark-submit append.py
spark-submit complete.py
spark-submit aggregate.py


############################
#SPARK SQL EXAMPLE
############################
spark-submit sqlQuery.py


############################
#USER DEFINED EXAMPLE
############################
spark-submit UDF.py

############################
#GROUPING AND AGGREGATION
############################
spark-submit timestamp_grouping.py
spark-submit aggregate_ratings_window.py


# Drop one file at a time into the datasets/droplocation directory


############################
#TWITTER PYTHON TWEET LISTNER
############################
# Start Twitter streaming app
python twitter.py localhost 9999 fifa nba 
python twitter.py localhost 9999 "world cup" nba 

############################
#TWITTER EXAMPLE PYTHON 
############################
# COUNT HAS TAG
spark-submit count_hash_tag.py localhost 9999
spark-submit count_hash_tag_window.py 9999


#COUNT HASH TAG WITH WINDOW
spark-submit join_batch_streaming.py
spark-submit join_batch_aggregate.py
spark-submit aggregate_ratings_window.py

############################
#KAFKA AND STRUCTURED STREAMING
############################

cd C:\kafka\kafka_2.13-2.5.0\

1) Run zookeeper
.\bin\windows\Zookeeper-server-start.bat .\config\zookeeper.properties

2) Run Kafka Broker
  .\bin\windows\kafka-server-start.bat .\config\server.properties

cd C:\kafka\kafka_2.13-2.5.0\bin\windows

3) Create Kafka Topics
# CREATE A KAFKA TOPIC CALLED first_kafka_topic
kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic first_kafka_topic

# LIST TOPICS
4) list Topics
kafka-topics.bat --list --zookeeper localhost:2181

# CREATE KAFKA PRODUCER
5) KAFKA PRODUCER
kafka-console-producer.bat --broker-list localhost:9090 --topic first_kafka_topic

# CREAET KAFKA CONSUMER
6) KAFKA CONSUMER
kafka-console-consumer.bat --bootstrap-server localhost:9090 --topic first_kafka_topic --from-beginning

# KAFKA HASH TAG TWEET LISTNER
python kafka_twitter_hashtag.py localhost 9090 first_kafka_topic "nba"

############################
#SENTIMENT ANALYSIS USING KAFKA AND STRUCTURED STREAMING
############################

# KAFKA SENTIMENT ANALYSIS USING AFIN ,PYKAFKA TWEEPY
python kafka_twitter_sentiment_analysis.py localhost 9090 first_kafka_topic "nba"

# SPARK STREAMING JOB WITH KAFKA PROVIDING SENTIMENT ANALYSIS ON LIVE STREAMS 
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 kafka_sentiment_analysis_streaming.py localhost 9090 first_kafka_topic

# Describe kafka topics
kafka-topics.bat --describe --topic first_kafka_topic --zookeeper localhost:2181
