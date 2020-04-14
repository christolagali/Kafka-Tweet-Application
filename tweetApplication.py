from tweepy import StreamListener,Stream,OAuthHandler

import threading
import logging
import time
import json
import sys


import producer.producer as prd
import config.config as cfg
import consumer.consumer as csm


def main():

    try:
        auth=OAuthHandler(cfg.authConfig['consume_api_key'], cfg.authConfig['consume_secret_api_key'])
        auth.set_access_token(cfg.authConfig['access_token'], cfg.authConfig['access_secret_token'])

        if sys.argv[1] == 'produce':
            for i in range(6):
                prd.produceTweet(auth)
                time.sleep(5)
                if i == 5:
                    break
        else:
            
            # consumer logic
            i =0 
            consumer = csm.consumeTweets()

            for msg in consumer:
                print(msg)
                time.sleep(5)
                i+=1

                if i==5:
                    break

    
    except Exception as e:
        logging.exception(e)



main()




    

