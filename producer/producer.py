from tweepy import StreamListener,Stream,OAuthHandler
from kafka import KafkaProducer

import threading
import logging
import time
import json


class TweetListener(StreamListener):

    def on_data(self,data):
        #boto3.kinesis.connect_to_region('us-east-1')
        
        try:
            
            producer = KafkaProducer(bootstrap_servers='localhost:9092',value_serializer=lambda v: json.dumps(v).encode('utf-8'))
            
            response = producer.send('new_tweet_topic',{"dataObjectID": data})
            
            print ("putting data")
            logging.info(response)
            return True
        except Exception:
            logging.exception("Problem pushing to kafka")

    def on_error(self, status):
        print (status)



def produceTweet(auth):

    try:

        twitter_stream = Stream(auth, TweetListener())
        twitter_stream.filter(track=["donald"])
    
    except Exception as e:
        logging.exception(e)


