#!/usr/bin/env python
# coding: utf-8

# In[3]:


import tweepy
import asyncio
from tweepy import StreamingClient, OAuthHandler, API
import socket
import json
import sys
import traceback
from os import getenv
from dotenv import load_dotenv

load_dotenv()
consumer_key = getenv('consumerKey')
consumer_secret = getenv('consumerSecret')
access_token = getenv('accessToken')
access_token_secret = getenv('accessTokenSecret')
bearer_token = getenv('bearerToken')

DEFAULT_HOST = "127.0.0.1"
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

    def on_data(self, json_tweet, should_close_stream=False):
        if should_close_stream is False:
            return False
        
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



def sendData(
        c_socket: str = DEFAULT_HOST, 
        stream_filter_list: list[str] = DEFAULT_STREAM_FILTERS) -> None:

    twitter_stream = TweetsListener(
        bearer_token,
        csocket=c_socket
    )
    """
    If you are using an Academic Research Project_ at the Basic access level, 
    you can use all available operators, can submit up to 1,000 concurrent rules, 
    and can submit rules up to 1,024 characters long.
    """
    print("Twitter Stream instantiated")
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

# @asyncio.coroutine
def startTweetStream(
        stream_filter_list: list[str] = DEFAULT_STREAM_FILTERS, 
        host: str="127.0.0.1", 
        port: int = 5554) -> None:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((host, port))
        print("Listening on port: %s" % str(port))
        s.listen(5)
        s.listen(5)                 # Now wait for client connection.
        c, addr = s.accept()        # Establish connection with client.
        print( "Tweet Stream received request from: " + str( addr ) )

        sendData( c , stream_filter_list)
        print("end of tweet stream function")
    except (KeyboardInterrupt, Exception):
        print(traceback.format_exc())
        print("\nShutting down socket")
        s.shutdown(socket.SHUT_RDWR)
        s.close()
        sys.exit()

def stopTweetStream():
    pass
    #sendData()

async def startTweetServer(
        host: str=DEFAULT_HOST, 
        port: int = 5554) -> None:
    print("\nStarting tweet server")
    server = await asyncio.start_server(handle_client, host, port)
    async with server:
        print("Tweet server up")
        await server.serve_forever()

## PLEASE WORK
# https://stackoverflow.com/questions/48506460/python-simple-socket-client-server-using-asyncio
import asyncio, socket

async def handle_client(reader, writer):
    request = None
    while request != 'quit':
        request = (await reader.read(255)).decode('utf8')
        response = str(eval(request)) + '\n'
        writer.write(response.encode('utf8'))
        await writer.drain()
    writer.close()

async def run_server():
    server = await asyncio.start_server(handle_client, 'localhost', 15555)
    async with server:
        await server.serve_forever()

#asyncio.run(run_server())

if __name__ == "__main__":
    # asyncio.run(startTweetServer())
    # print("cmon man ")
    # sendData()

    startTweetStream()