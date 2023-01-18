#!/usr/bin/python3

# imports
from kafka import KafkaProducer # pip install kafka-python
# import numpy as np              # pip install numpy
# from sys import argv, exit
from time import time, sleep
import tweepy
from datetime import datetime

consumer_key ='ZCnw5kVGxN1XAt7YxasvOwRI9'
consumer_secret = 'TO5E7st4ZVaYWSGOAEjHoSbMvqmUvoeBMz2JlgLo7T3yqZOO3u'
access_token = '1526400686942199808-9TdAiwdXBIhSsZcoUSn08KynnBUoMb'
access_token_secret = 'KKvOFMh8jrbDDjxoAFTc3al4EBuXH0glLzVIyFkKBgTis'

#creating the authen object
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

#creating the api object by passing in auth information
api = tweepy.API(auth, wait_on_rate_limit=True, proxy="socks5://localhost:9050")

# set up the producer
producer = KafkaProducer(bootstrap_servers='localhost:9092')
topic_name = "demo_twitter_1"

def normalize_timestamp(time):
    i = time.find("+")
    time_1 = time[:i]
    my_time = datetime.strptime(time_1, '%Y-%m-%d %H:%M:%S')
    return (my_time.strftime("%Y-%m-%d %H:%M:%S"))

def get_twitter_data():
    res = tweepy.Cursor(api.search_tweets,q="bitcoin",lang="en").items()
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