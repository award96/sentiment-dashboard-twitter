"""
Refer to this

https://flask.palletsprojects.com/en/2.2.x/tutorial/layout/
"""

import os
from flask import Flask, redirect, url_for, render_template, request, jsonify
# this is how you would import a submodule
from flask_root.twitter import get_twitter_data
HOME_PAGE = 'home'
RESULTS_PAGE = 'results'

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
        print("\n\nHXXXXXXHXHXHXHXHXHHXHXHXHXHXHXHXHX")
        print(url_for(HOME_PAGE))
        return redirect(url_for(HOME_PAGE))

    
    @app.route('/' + HOME_PAGE, methods=['GET', 'POST'])
    def post_query():
        query = request.form['query']
        res = get_twitter_data(query)
        if res is None:
            return render_template("404.html")
        return render_template("results.html", data=res)

    @app.route('/' + RESULTS_PAGE)
    def display_query_res():
        return render_template("results.html")
    
    

    return app