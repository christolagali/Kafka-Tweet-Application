import tweepy
from kafka import KafkaProducer

import threading
import logging
import time
import json


class tweetlistener(tweepy.StreamListener):
    # This is a class provided by tweepy to access the Twitter Streaming API.

    def on_connect(self):
        # Called initially to connect to the Streaming API
        print("You are now connected to the streaming API.")

    def on_error(self, status_code):
        # On error - if an error occurs, display the error / status code
        print("Error received in kafka producer " + repr(status_code))
        return True # Don't kill the stream

    def on_data(self, data):
        """ This method is called whenever new data arrives from live stream.
        We asynchronously push this data to kafka queue"""
        try:
            producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
            producer.send('my_twtopic', data.encode('utf-8'))
        except Exception as e:
            print(e)
            return False
        return True # Don't kill the stream

    def on_timeout(self):
        return True # Don't kill the stream


