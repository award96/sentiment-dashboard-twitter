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
import sys


# create a SparkSession
spark = SparkSession.builder.appName("TwitterStreaming").getOrCreate()

# Check if there is an active StreamingContext
existing_context = StreamingContext.getActive()
if existing_context is not None:
    # Stop the existing StreamingContext
    existing_context.stop()

# create a StreamingContext with batch interval of 10 second
ssc = StreamingContext(spark.sparkContext, 10)
sqlContext = SQLContext(spark)
spark.sparkContext.setLogLevel("ERROR")

# create a DStream that reads data from port 5554
socket_stream = ssc.socketTextStream("127.0.0.1", 5554)
lines = socket_stream.window(20)


# define the schema for your DataFrame
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


ssc.start()

import time
from IPython import display
import matplotlib.pyplot as plt
import pandas as pd
from textblob import TextBlob
from wordcloud import WordCloud, STOPWORDS


#Define function

# define a function to get sentiment polarity for each sentence
def get_sentiment(text):
    blob = TextBlob(text)
    return blob.sentiment.polarity

count = 0
prev_sentiment_percentages = pd.Series([0,0,0,0], index=['[-1.0, -0.5]', '(-0.5, 0.0]', '(0.0, 0.5]', '(0.5, 1.0]'])

while count < 2:
    display.clear_output(wait=True)
    time.sleep(5)
    latest_25 = sqlContext.sql('SELECT * FROM tweets_table LIMIT 25')
    sent_df = latest_25.toPandas()
    print(sent_df)
    
    # apply the function to your dataframe to get sentiment for each sentence
    sent_df['sentiment'] = sent_df['tweet'].apply(get_sentiment)

    # group sentiment by range and count the number of sentences in each range
    bins = pd.cut(sent_df['sentiment'], bins=[-1, -0.5, 0, 0.5, 1])
    sentiment_counts = sent_df.groupby(bins)['sentiment'].count()

    # calculate percentage of sentences in each sentiment range
    sentiment_percentages = sentiment_counts / len(sent_df) * 100

    
    # plot the results with custom labels for each range
    plt.figure(figsize=(6,4))
    ax = sentiment_percentages.plot(kind='bar')
    ax.set_title('Sentiment Analysis Results')
    ax.set_xlabel('Sentiment Range')
    ax.set_ylabel('Percentage of Sentences')
    ax.set_xticklabels(['Strongly Negative', 'Somewhat Negative', 'Somewhat Positive', 'Strongly Positive'])

    #Calculate change from previous
    percentage_changes = ((sentiment_percentages - prev_sentiment_percentages) / prev_sentiment_percentages) * 100

    plt.title('Percentage of Sentences per Sentiment for Latest 100 tweets')
    plt.savefig('PLOT.png')
    plt.show()
    
    # clear previous word cloud plot
    plt.clf()
    
    # display the word cloud
    plt.figure(figsize=(6,6))
    wordcloud = WordCloud(width=450, height=450, background_color='white', max_words=50, colormap='magma').generate_from_text(' '.join(sent_df['tweet']))
    plt.imshow(wordcloud)
    plt.axis('off')
    plt.show()
    plt.savefig('WORDCLOUD.png')
    
    count += 1
    prev_sentiment_percentages = sentiment_percentages
    time.sleep(5)

    ssc.stop()
    sys.exit()