import sys
import traceback
import SparkStream
import SparkSentiment

def start():
    scc = None
    try:
        sqlContext, scc = SparkStream.startSpark()
        SparkSentiment.main(sqlContext)
    except (KeyboardInterrupt, Exception):
        print(traceback.format_exc())
        SparkStream.stopSpark(scc)
        sys.exit()



if __name__ == "__main__":
    start()