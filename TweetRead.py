#!/usr/bin/env python
# coding: utf-8

# In[3]:


import tweepy
from tweepy import StreamingClient, OAuthHandler, API
import socket
import json
from os import getenv
from dotenv import load_dotenv

load_dotenv()
consumer_key = getenv('consumerKey')
consumer_secret = getenv('consumerSecret')
access_token = getenv('accessToken')
access_token_secret = getenv('accessTokenSecret')
bearer_token = getenv('bearerToken')

CA_SNOW_SEARCH = "lang:en -is:retweet (\"california snow\" OR \"cali snow\") february" # english language, no retweets, 
UCLA_SEARCH = "lang:en -is:retweet ucla OR #ucla OR @ucla"
RONALDO_SEARCH = "to:Cristiano OR @Cristiano lang:en"
VERIFIED_SEARCH = "is:verified lang:en"
DEFAULT_STREAM_FILTERS = [CA_SNOW_SEARCH, 
                          UCLA_SEARCH,
                          RONALDO_SEARCH,
                          VERIFIED_SEARCH]

class TweetsListener(StreamingClient):
    # https://www.youtube.com/watch?v=8r5en18DOZQ
    # https://stackoverflow.com/questions/70581159/typeerror-init-missing-2-required-positional-arguments-access-token-an
    def __init__(self, *args, csocket):
        super().__init__(*args)
        self.client_socket = csocket
    def on_connect(self):
        print("Twitter API connected")

    def on_tweet(self, tweet):
        if tweet.referenced_tweets is None:
            # tweet is not a reply 
            pass

    def on_data(self, json_tweet):
        try:
            tweet = json.loads(json_tweet)
            data = tweet['data']

            msg = data['text'] + '\n'

            print( msg.encode('utf-8') )
            self.client_socket.send( msg.encode('utf-8') )
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
            return True

    def on_error(self, status):
        print(status)
        return True



def sendData(c_socket, stream_filter_list = DEFAULT_STREAM_FILTERS):

    twitter_stream = TweetsListener(
        bearer_token,
        csocket=c_socket
    )
    """
    If you are using an Academic Research Project_ at the Basic access level, 
    you can use all available operators, can submit up to 1,000 concurrent rules, 
    and can submit rules up to 1,024 characters long.
    """
    # twitter api has persistent rules
    current_rules = twitter_stream.get_rules()
    to_delete = [x.id for x in current_rules.data]
    twitter_stream.delete_rules(to_delete)
    # enforce new rules
    for stream_filter in stream_filter_list:
        twitter_stream.add_rules(tweepy.StreamRule(stream_filter))

    # check
    print(f"\nFiltering with the following rules {twitter_stream.get_rules()}\n")
    twitter_stream.filter()
    
if __name__ == "__main__":
    s = socket.socket()         # Create a socket object
    host = "127.0.0.1"     # Get local machine name
    port = 5554                 # Reserve a port for your service.
    s.bind((host, port))        # Bind to the port

    print("Listening on port: %s" % str(port))

    s.listen(5)                 # Now wait for client connection.
    c, addr = s.accept()        # Establish connection with client.

    print( "Received request from: " + str( addr ) )

    sendData( c )