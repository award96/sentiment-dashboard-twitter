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

async def start():
    scc = None
    try:
        asyncio.run(TweetRead.startTweetServer())
        TweetRead.sendData()
        sqlContext, scc = SparkStream.startSpark()
        SparkSentiment.main(sqlContext)
    except:
        print(traceback.format_exc())
        stop(scc)
        quit()

if __name__ == "__main__":
    asyncio.run(start())


