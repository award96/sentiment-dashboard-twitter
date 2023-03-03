import traceback
import asyncio
from twitterMethods import TweetRead
from sparkMethods import SparkSentiment, SparkStream


# async def start():
#     tweet = loop.create_task(start_tweet_stream())
#     spark = loop.create_task(start_spark_stream())
#     await asyncio.wait([tweet, spark])


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
            SparkStream.startSpark(),
            TweetRead.startTweetStream(),
            SparkSentiment.main()
        ))
    except:
        print(traceback.format_exc())
        loop.close()
        quit()


