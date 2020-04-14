from tweepy import StreamListener,Stream,OAuthHandler
from kafka import KafkaConsumer

import threading
import logging
import time
import json



def consumeTweets():

    consumer = KafkaConsumer('new_first_topic',bootstrap_servers='localhost:9092')

    return consumer

