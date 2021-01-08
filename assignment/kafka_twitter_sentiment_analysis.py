import sys
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import json
import pykafka

class TweetsListener(StreamListener):

    def __init__(self, kafkaProducer):
        print ("Sentiment analysis")
        self.producer = kafkaProducer
    def on_data(self, data):
        

    def on_error(self, status):
        print(status)
        return True

def connect_to_twitter(connection, tracks):

    
    
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

    

    #initialize kafka client
    kafkaClient = pykafka.KafkaClient(host + ":" + port)

    # pass topic as comman line argument
    kafkaProducer = kafkaClient.topics[bytes(topic,"utf-8")].get_producer()

    # Connect to twitter
    connect_to_twitter(kafkaProducer, tracks)






















































































































































