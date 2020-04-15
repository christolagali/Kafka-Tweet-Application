from tweepy import StreamListener,Stream,OAuthHandler

import threading
import logging
import time
import json
import sys
import tweepy


import producer.producer as prd
import config.config as cfg
import consumer.consumer as csm


def main():

    try:
        auth=OAuthHandler(cfg.authConfig['consume_api_key'], cfg.authConfig['consume_secret_api_key'])
        auth.set_access_token(cfg.authConfig['access_token'], cfg.authConfig['access_secret_token'])
        #WORDS = ['bitcoin', 'Bitcoin', '#Bitcoin', '#bitcoin', 'BTC', '#BTC', 'btc', '#btc']
        WORDS = ['trump']
        # producer logic
        if sys.argv[1] == 'produce':
            api = tweepy.API(auth)

            # Set up the listener. The 'wait_on_rate_limit=True' is needed to help with Twitter API rate limiting.
            listener = prd.tweetlistener(api=tweepy.API(wait_on_rate_limit=True, wait_on_rate_limit_notify=True, timeout=60, retry_delay=5, retry_count=10, retry_errors=set([401, 404, 500, 503])))
            stream = tweepy.Stream(auth=auth, listener=listener)
            print("Tracking: " + str(WORDS))
            stream.filter(track=WORDS, languages = ['en'])
        else:
            
            # consumer logic
            i =0 
            consumer = csm.consumeTweets()
            print('I am now consuming!')
            for msg in consumer:
                print(msg)
                time.sleep(5)
                i+=1

                if i==5:
                    break

    
    except Exception as e:
        logging.exception(e)



main()




    

