
import requests
import os
import json
from os import getenv
from dotenv import load_dotenv

MAX_TWEETS = 24
load_dotenv()

# To set your enviornment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
consumer_key = getenv('consumerKey')
consumer_secret = getenv('consumerSecret')
access_token = getenv('accessToken')
access_token_secret = getenv('accessTokenSecret')
bearer_token = getenv('bearerToken')


def bearer_oauth(r):
    print("bearer_oath")
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2FilteredStreamPython"
    return r


def get_rules():
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", auth=bearer_oauth
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print("get_rules()")
    print(json.dumps(response.json()))
    return response.json()


def delete_all_rules(rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    print("delete_all_rules")
    print(json.dumps(response.json()))


def set_rules(search_term: str):
    # You can adjust the rules if needed
    sample_rules = [
        {"value": f"lang:en -is:retweet #{search_term}"},
    ]
    payload = {"add": sample_rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print("set_rules")
    print(json.dumps(response.json()))


def get_stream():
    tweet_count=0
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream", auth=bearer_oauth, stream=True,
    )
    print("get_stream")
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    for response_line in response.iter_lines():
        if response_line:
            while tweet_count <= MAX_TWEETS:
                tweet_count += 1
                json_response = json.loads(response_line)
                res = json.dumps(json_response, indent=4, sort_keys=True)
                #print(res)
                yield res


def main(search_term: str = "Oscars"):
    print("main")
    rules = get_rules()
    delete_all_rules(rules)
    set_rules(search_term)
    tc = 0
    for tweet in get_stream():
        tc+=1
        if tc > MAX_TWEETS:
            break
        print(tweet)
        yield tweet


if __name__ == "__main__":
    main()
