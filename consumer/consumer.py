from tweepy import StreamListener,Stream,OAuthHandler
from kafka import KafkaConsumer

import threading
import logging
import time
import json



def consumeTweets():

    consumer = KafkaConsumer('new_tweet_topic',bootstrap_servers=['localhost:9092'],auto_offset_reset='smallest', group_id='my-group',enable_auto_commit=True#,value_deserializer=lambda x: loads(x.decode('utf-8'))
                            
                        )
    return consumer

