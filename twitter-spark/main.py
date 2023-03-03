import traceback
import asyncio
from twitterMethods import TweetRead
from sparkMethods import SparkSentiment, SparkStream

async def start_tweet_stream():
    await TweetRead.startTweetStream()
async def start_spark_stream():
    await SparkStream.startSpark()



async def start():
    tweet = loop.create_task(start_tweet_stream())
    spark = loop.create_task(start_spark_stream())
    await asyncio.wait([tweet, spark])


    # while TweetRead.startTweetStream():
    #     scc = SparkStream.startSpark()
    #     SparkSentiment.main()
    #     return scc

def stop(scc):
    SparkStream.stopSpark(scc)
    TweetRead.stopTweetStream()


if __name__ == "__main__":
    scc = None
    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.gather(
            start_tweet_stream(),
            start_spark_stream()
        ))
    except:
        print(traceback.format_exc())
        loop.close()
        quit()


