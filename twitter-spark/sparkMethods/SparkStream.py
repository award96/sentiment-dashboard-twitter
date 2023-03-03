from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import explode, split, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
import matplotlib.pyplot as plt
import re
import time
import warnings
warnings.filterwarnings('ignore')
warnings.simplefilter('ignore')

async def startSpark():
    print("building spark stream")
    # create a SparkSession
    spark = SparkSession.builder.appName("TwitterStreaming").getOrCreate()

    # create a StreamingContext with batch interval of 10 second
    ssc = StreamingContext(spark.sparkContext, 10)
    sqlContext = SQLContext(spark)
    spark.sparkContext.setLogLevel("ERROR")
    print("connecting spark to socket")
    # create a DStream that reads data from port 5554
    socket_stream = ssc.socketTextStream("127.0.0.1", 5554)
    lines = socket_stream.window(20)

    # define the schema for your DataFrame
    print("defining schema for spark")
    schema = StructType([
        StructField("tweet", StringType(), True)
    ])

    (lines.map(lambda x: x.split("t_end"))
    .flatMap(lambda x: x)
    .map(lambda x: x.strip())
    .filter(lambda x: len(x) > 0)
    .map(lambda x: re.sub(r'http\S+', '', x))
    .map(lambda x: re.sub(r'@\w+', '', x))
    .map(lambda x: re.sub(r'#', '', x))
    .map(lambda x: re.sub(r'RT', '', x))
    .map(lambda x: re.sub(r':', '', x))
    .foreachRDD(lambda rdd: rdd.map(lambda x: (x,)).toDF(schema).limit(25).createOrReplaceTempView("tweets_table"))
    )

    # start the context
    print("starting spark stream")
    ssc.start()
    return ssc 

def stopSpark(ssc):
    # Stop the SparkSession. MAKE SURE TO RUN THIS AFTER YOU ARE DONE!!
    ssc.stop()