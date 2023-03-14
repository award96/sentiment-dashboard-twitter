"""
Refer to this

https://flask.palletsprojects.com/en/2.2.x/tutorial/layout/
"""

import os
from flask import Flask, redirect, url_for, render_template, request, jsonify, Response
import itertools
# this is how you would import a submodule
from flask_root import stream
import json
import matplotlib.pyplot as plt
import pandas as pd
from textblob import TextBlob
from wordcloud import WordCloud, STOPWORDS
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
import base64
from io import BytesIO
from PIL import Image
from os import path
import numpy as np

n_tweets = 10
HOME_PAGE = 'home'
RESULTS_PAGE = 'results'
custom_stopwords = STOPWORDS
stop_words = ['RT', 'co', 't', 'https'] + list(STOPWORDS)

def get_sentiment(text):
    blob = TextBlob(text)
    return blob.sentiment.polarity

def get_plots(data, search_term):
    # data input is a list of dictionaries
    sent_df = pd.DataFrame.from_dict(data)
    
    # apply the function to your dataframe to get sentiment for each sentence
    sent_df['sentiment'] = sent_df['text'].apply(get_sentiment)

    # group sentiment by range and count the number of sentences in each range
    bins = pd.cut(sent_df['sentiment'], bins=[-1, -0.5, 0, 0.5, 1])
    sentiment_counts = sent_df.groupby(bins)['sentiment'].count()

    # calculate percentage of sentences in each sentiment range
    sentiment_percentages = (sentiment_counts / len(sent_df) * 100).to_list()

    
    # plot the results with custom labels for each range
    fig = Figure()
    ax = fig.add_subplot(1,1,1)
    ax.set_title('Sentiment Analysis Results')
    ax.set_xlabel('Sentiment Range')
    ax.set_ylabel('Percentage of Sentences')
    ax.set_xticks([0, 1, 2, 3])
    ax.set_xticklabels(['Strongly Negative', 'Somewhat Negative', 'Somewhat Positive', 'Strongly Positive'])
    ax.plot(sentiment_percentages)

    fig2 = Figure()
    ax2 = fig2.add_subplot(1,1,1)
    mask = np.array(Image.open("flask_root/static/Twitter-logo.png"))[:,:,0]
    def transform_zeros(val):
        if val == 0: 
            return 255
        else:
            return val

    twitter_logo = np.ndarray((mask.shape[0],mask.shape[1]), np.int32)

    for i in range(len(mask)):
        twitter_logo[i] = list(map(transform_zeros, mask[i]))
    wordcloud = WordCloud(width=450, height=450, background_color='white', max_words=50, 
                          mask=twitter_logo, colormap='magma', stopwords = stop_words + search_term.split(' '),
                          contour_width=2, contour_color='#1477b5').generate_from_text(' '.join(sent_df['text']))
    ax2.imshow(wordcloud)

    return fig, fig2

def create_app(test_config=None):
    # create and configure the app
    app = Flask(__name__, instance_relative_config=True)
    app.config.from_mapping(
        SECRET_KEY='dev',
        DATABASE=None,
    )

    if test_config is None:
        # load the instance config, if it exists, when not testing
        app.config.from_pyfile('config.py', silent=True)
    else:
        # load the test config if passed in
        app.config.from_mapping(test_config)

    # ensure the instance folder exists
    try:
        os.makedirs(app.instance_path)
    except OSError:
        pass
    
    # The website

    # LANDING PAGE
    @app.route('/' + HOME_PAGE)
    def home():
        return render_template("index.html")

    # 404 redirects to LANDING PAGE
    @app.errorhandler(404)
    def page_not_found(e):
        return go_to_landing()
    
    # EMPTY ROUTE redirects to LANDING PAGE
    @app.route('/')
    def go_to_landing():
        return redirect(url_for(HOME_PAGE))

    
    @app.route('/' + HOME_PAGE, methods=['GET', 'POST'])
    def post_query():
        query = request.form['query']
        res = []
        t_count = 0
        for tweet in stream.main(query):
            #display tweet
            tweet_dict = json.loads(tweet)

            if 'data' in tweet_dict and 'text' in tweet_dict['data']:
                res.append(tweet_dict['data'])
            
            t_count += 1
            if t_count > n_tweets:
                break
            
        if res is None or res == []:
            return render_template("404.html")
        
        sentiment, wordcloud = get_plots(res, query)
        buf = BytesIO()
        FigureCanvas(sentiment).print_png(buf)
    
        # Encode PNG image to base64 string
        sentimentB64String = "data:image/png;base64,"
        sentimentB64String += base64.b64encode(buf.getvalue()).decode('utf8')

        buf2 = BytesIO()
        FigureCanvas(wordcloud).print_png(buf2)
        wordcloudB64String = "data:image/png;base64,"
        wordcloudB64String += base64.b64encode(buf2.getvalue()).decode('utf8')
        return render_template("results.html", sentiment=sentimentB64String, wordcloud = wordcloudB64String)

    @app.route('/' + RESULTS_PAGE)
    def display_query_res():
        return render_template("results.html")
    
    return app