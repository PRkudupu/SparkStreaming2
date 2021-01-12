# DEMO1
# Start netcat utility
nc -L -p 9999
nc localhost 9999

# Run Python code to read stream from socket and write to console
spark-submit streaming.py localhost 9999

# Input sentences
Baked chicken is healthier than fried chicken
I prefer fried fish over fried chicken

cd C:\Users\tekstudent\Desktop\tutorials\code

#DEMO2
# Start streaming app
spark-submit append.py
spark-submit complete.py
spark-submit aggregate.py
spark-submit sqlQuery.py
spark-submit UDF.py
spark-submit timestamp_grouping.py
spark-submit aggregate_ratings_window.py


# Drop one file at a time into the datasets/droplocation directory

# Start Twitter streaming app
python twitter.py localhost 9999 fifa nba 
python twitter.py localhost 9999 "world cup" nba 

#TWITTER EXAMPLES
spark-submit count_hash_tag.py localhost 9999
spark-submit count_hash_tag_window.py 9999


#JOIN
spark-submit join_batch_streaming.py
spark-submit join_batch_aggregate.py
spark-submit aggregate_ratings_window.py


cd C:\kafka\kafka_2.13-2.5.0\


1) Run zookeeper
.\bin\windows\Zookeeper-server-start.bat .\config\zookeeper.properties

2) Run Kafka Broker
  .\bin\windows\kafka-server-start.bat .\config\server.properties

cd C:\kafka\kafka_2.13-2.5.0\bin\windows

3) Create Kafka Topics
# CREATE A KAFKA TOPIC called first_kafka_topic
kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic first_kafka_topic

4) list Topics
kafka-topics.bat --list --zookeeper localhost:2181

5) KAFKA PRODUCER
kafka-console-producer.bat --broker-list localhost:9092 --topic first_kafka_topic

6) KAFKA CONSUMER
kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic first_kafka_topic --from-beginning

7) DELETE KAFKA TOPIC
# KAFKA DELETE A TOPIC
kafka-topics. --delete --zookeeper localhost:2181 --topic first_kafka_topic

cd C:\Users\tekstudent\Desktop\tutorials\code
# Run Kafka Tweet Producer HASH TAG
python kafka_twitter_hashtag.py localhost 9092 first_kafka_topic "nba"

# Run Kafka Tweet Producer HASH TAG
python kafka_twitter_sentiment_analysis.py localhost 9092 first_kafka_topic "nba"


# Run Kafka Consumer
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4 kafka_sentiment_analysis_streaming.py localhost 9092 first_kafka_topic

#Describe kafka topics
kafka-topics.bat --describe --topic first_kafka_topic --zookeeper localhost:2181

##LAB 2
cd C:\kafka\kafka_2.13-2.5.0\

1) Run zookeeper
.\bin\windows\Zookeeper-server-start.bat .\config\zookeeper.properties

2) Run Kafka Broker
  .\bin\windows\kafka-server-start.bat .\config\server.properties

cd C:\kafka\kafka_2.13-2.5.0\bin\windows

# START 3 BROKERS
3)
.\bin\windows\kafka-server-start.bat .\config\server-01.properties

.\bin\windows\kafka-server-start.bat .\config\server-02.properties

.\bin\windows\kafka-server-start.bat .\config\server-03.properties

# CREATE TOPIC WITH REPLICATION FACTOR AS 3

4) 
kafka-topics.bat --create --zookeeper localhost:2181 --replication-factor 3 --partitions 1 --topic replicated_3_topic

#look for the topic
5) 
kafka-topics.bat --describe --topic replicated_3_topic --zookeeper localhost:2181

#CREATE A PRODUCER
6)
kafka-console-producer.bat --broker-list localhost:9092 --topic replicated_3_topic

#CREATE A CONSUMER
7)
kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic replicated_3_topic --from-beginning

## LAB 3
# CHANGE DIRECTORY
cd C:\kafka\kafka_2.13-2.5.0\
# RUN ZOOKEEPER
1) Run zookeeper
.\bin\windows\Zookeeper-server-start.bat .\config\zookeeper.properties

# RUN KAFKA BROKER
2) Run Kafka Broker
  .\bin\windows\kafka-server-start.bat .\config\server.properties
#CHANGE DIRECTORY
cd C:\kafka\kafka_2.13-2.5.0\bin\windows


