#!/usr/bin/python3

# imports
from kafka import KafkaProducer # pip install kafka-python
# import numpy as np              # pip install numpy
# from sys import argv, exit
from time import time, sleep
import tweepy
from datetime import datetime

consumer_key ='CgtM4LSXLuzOwny3MkIvHfcIB'
consumer_secret = 'fGrKVtT9KEn3oAISalJ6cwR6Ke55QuIdXLEonpWBocdrHug7ln'
access_token = '1526400686942199808-kKa0VFkhnu3SgGJAMww54n7taOLTxw'
access_token_secret = 'BBdyygvto1eRB3OpNtp2Y6C15deE4ROiHrsEaZgkJPrve'

# consumer_key ='2Rc3dZGMoaOeTiuDw4KzM3mYe'
# consumer_secret = 'BHr3rkRv64LMSvgnA3oi2yFitoHcEPqeDdoJYcIU61B8X3SDW7'
# access_token = '1526400686942199808-fOMODDwtwWdsHngPOdDZR2NJqAbvB1'
# access_token_secret = 'QNqqmY1ny5Yyjm7Ozrm9SBQCnDYlPCOW4OVcJsLhejqae'

#creating the authen object
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

#creating the api object by passing in auth information
api = tweepy.API(auth, wait_on_rate_limit=True)

# set up the producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')
topic_name = "demo_twitter_1"

def normalize_timestamp(time):
    i = time.find("+")
    time_1 = time[:i]
    my_time = datetime.strptime(time_1, '%Y-%m-%d %H:%M:%S')
    return (my_time.strftime("%Y-%m-%d %H:%M:%S"))

def get_twitter_data():
    res = tweepy.Cursor(api.search_tweets,q="presidentialdebate",lang="en").items()
    for i in res:
        msg = f'{time()},;,{str(i.user.screen_name)},;,{str(normalize_timestamp(str(i.created_at)))},;, {str(i.user.followers_count)},;,{str(i.user.location)},;, {str(i.favorite_count)},;, {str(i.retweet_count)},;, {str(i.source)},;, {str(i.text)},;,{str(i.user.statuses_count)},;,'
        producer.send(topic_name, bytes(msg, encoding='utf8'))

def periodic_work(interval):
    while True:
        get_twitter_data()

        #interval should be an integer, the number of seconds to wait
        sleep(interval)

periodic_work(10)
# print(now)
