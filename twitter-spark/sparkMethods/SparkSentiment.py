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

#Load Libraries
import time
from IPython import display
import matplotlib.pyplot as plt
import pandas as pd
from textblob import TextBlob
from wordcloud import WordCloud, STOPWORDS


def get_sentiment(text):
    blob = TextBlob(text)
    return blob.sentiment.polarity

def main():
    count = 0
    prev_sentiment_percentages = pd.Series([0,0,0,0], index=['[-1.0, -0.5]', '(-0.5, 0.0]', '(0.0, 0.5]', '(0.5, 1.0]'])

    while count < 5:
        display.clear_output(wait=True)
        time.sleep(5)
        latest_25 = sqlContext.sql('SELECT * FROM tweets_table LIMIT 25')
        sent_df = latest_25.toPandas()
        
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

        
        
        # add percentage change labels to each bar (STILL WORKING ON THIS)
    #     for i, v in enumerate(sentiment_percentages):
    #         ax.text(i - 0.1, v + 0.5, f'{v:.2f}%', fontsize=10)
    #         if percentage_changes[i] > 0:
    #             color = 'green'
    #             sign = '+'
    #         else:
    #             color = 'red'
    #             sign = ''
    #         ax.text(i - 0.1, v - 5, f'{sign}{percentage_changes[i]:.2f}%', fontsize=10, color=color)



        plt.title('Percentage of Sentences per Sentiment for Latest 100 tweets')
        plt.show()
        
        # clear previous word cloud plot
        plt.clf()
        
        # display the word cloud
        plt.figure(figsize=(6,6))
        wordcloud = WordCloud(width=450, height=450, background_color='white', max_words=50, colormap='magma').generate_from_text(' '.join(sent_df['tweet']))
        plt.imshow(wordcloud)
        plt.axis('off')
        plt.show()
        
        count += 1
        prev_sentiment_percentages = sentiment_percentages
        time.sleep(5)