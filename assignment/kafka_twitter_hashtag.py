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
       
        return True

    def on_error(self, status):
        print(status)
        return True

def connect_to_twitter(connection, tracks):

    
if __name__ == "__main__":

    
    #initialize kafka client
    kafkaClient = pykafka.KafkaClient(host + ":" + port)

    # pass topic as comman line argument
    kafkaProducer = kafkaClient.topics[bytes(topic,"utf-8")].get_producer()

    # Connect to twitter
    connect_to_twitter(kafkaProducer, tracks)






















































































































































