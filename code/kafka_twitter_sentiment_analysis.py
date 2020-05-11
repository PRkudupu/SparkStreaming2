import sys
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import json
import pykafka

class TweetsListener(StreamListener):

    def __init__(self, kafkaProducer):
        print ("Tweets producer initialized")
        self.producer = kafkaProducer
    def on_data(self, data):
        try:
            json_data = json.loads(data)
            tweet = json_data["text"]
            print(tweet +"\n")   
            #1. Send the tweet to kafka producer
            #2. Convert the tweet to json
            self.producer.produce(bytes(json.dumps(tweet),"ascii"))

        except KeyError as e:
            print("Error on_data: %s" % str(e))

        return True

    def on_error(self, status):
        print(status)
        return True

def connect_to_twitter(connection, tracks):

    api_key = "AyRj5WdMHlWL40n2yblHcTt4w"
    api_secret = "DdX3O6SrBYU9reYNzCeEcy9RIepFZYZWZzRLOpFSRUjlrIxNfT"

    access_token = "4317207928-9LYbCyI0EhG9GxkFTMxBvXqqElWJMvqI4d3Ktr1"
    access_token_secret = "DxY9jKd20iAWTvzU8gWhV4Gi9jpWH2L4KjcqiGXb2zNRY"
    
    #OAuthHandler
    auth = OAuthHandler(api_key, api_secret)
    auth.set_access_token(access_token, access_token_secret)

    # Takes kafka producer as argument as it would send these tweets to kafka
    twitter_stream = Stream(auth, TweetsListener(kafkaProducer))
    twitter_stream.filter(track=tracks, languages=["en"])

if __name__ == "__main__":

    if len(sys.argv) < 5:
        print("Usage: python m03_demo02_kafkaHashtagProducer.py <host> <port> <topic_name> <tracks>", 
                file=sys.stderr)
        exit(-1)

    host = sys.argv[1]
    port = sys.argv[2]
    topic = sys.argv[3]
    tracks = sys.argv[4:]

    #initialize kafka client
    kafkaClient = pykafka.KafkaClient(host + ":" + port)

    # pass topic as comman line argument
    kafkaProducer = kafkaClient.topics[bytes(topic,"utf-8")].get_producer()

    # Connect to twitter
    connect_to_twitter(kafkaProducer, tracks)






















































































































































