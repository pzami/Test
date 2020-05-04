import tweepy
from google.cloud import pubsub_v1
from tweepy.streaming import StreamListener
import time
import json
import datetime
import os
# --------------Start- Configure and write data to Google Cloud PubSub

# Configure the connection with Google Cloud PubSub


project = "timeseries-264820"
pubsub_topic = "timeserie"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project, pubsub_topic)

# Function to write data to
def write_to_pubsub(data):
    try:
        if data["lang"] == "en":
          
            # publish to the topic, don't forget to encode everything at utf8!
            publisher.publish(topic_path, data=json.dumps({
                "text": data["text"],
                "user_id": data["user_id"],
                "id": data["id"],
                "posted_at": datetime.datetime.fromtimestamp(data["created_at"]).strftime('%Y-%m-%d %H:%M:%S')
            }).encode("utf-8"), tweet_id=str(data["id"]).encode("utf-8"))
            print(json.dumps({"text": data["text"],"user_id": data["user_id"],"id": data["id"],"posted_at": datetime.datetime.fromtimestamp(data["created_at"]).strftime('%Y-%m-%d %H:%M:%S')}).encode("utf-8"))
    except Exception as e:
        print(e)
        raise
# Method to format a tweet from tweepy
def reformat_tweet(tweet):
    """

    :param tweet:
    :return:
    """
    x = tweet

    processed_doc = {
        "id": x["id"],
        "lang": x["lang"],
        "retweeted_id": x["retweeted_status"][
            "id"] if "retweeted_status" in x else None,
        "favorite_count": x["favorite_count"] if "favorite_count" in x else 0,
        "retweet_count": x["retweet_count"] if "retweet_count" in x else 0,
        "coordinates_latitude": x["coordinates"]["coordinates"][0] if x[
            "coordinates"] else 0,
        "coordinates_longitude": x["coordinates"]["coordinates"][0] if x[
            "coordinates"] else 0,
        "place": x["place"]["country_code"] if x["place"] else None,
        "user_id": x["user"]["id"],
        "created_at": time.mktime(
            time.strptime(x["created_at"], "%a %b %d %H:%M:%S +0000 %Y"))
    }

    if x["entities"]["hashtags"]:
        processed_doc["hashtags"] = [
            {"text": y["text"], "startindex": y["indices"][0]} for y in
            x["entities"]["hashtags"]]
    else:
        processed_doc["hashtags"] = []

    if x["entities"]["user_mentions"]:
        processed_doc["usermentions"] = [
            {"screen_name": y["screen_name"], "startindex": y["indices"][0]} for
            y in
            x["entities"]["user_mentions"]]
    else:
        processed_doc["usermentions"] = []

    if "extended_tweet" in x:
        processed_doc["text"] = x["extended_tweet"]["full_text"]
    elif "full_text" in x:
        processed_doc["text"] = x["full_text"]
    else:
        processed_doc["text"] = x["text"]

    return processed_doc
# --------------End- Configure and write data to Google Cloud PubSub


# Authenticate with Twitter
consumer_key="PBecjcuxbk2VMYw7L2AJzcasY"
consumer_secret="oEqqRv2h10Vsy1thfSPR3j1XtDSQOKIBepe9jHECsHv8ceBuDM"

access_token="812779533153746944-noINNEMrnvFh1wOVfxZj8SaSEeoT2xI"
access_token_secret="Ui3Zz3seNbWZ1ZOdKdxxQnDY0Tf7RaZuvMKObUioGk3CW"

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

# Configure to wait on rate limit if necessary
api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=False)

# Hashtag list
lst_hashtags = ["#Iran","#iran", "#Trump","#trump", "#IranWar", "#iranwar4trump2020"]

# Listener class
class TweetListener(StreamListener):

    def __init__(self):
        super(TweetListener, self).__init__()

    def on_status(self, data):
        # When receiveing a tweet: send it to pubsub
        write_to_pubsub(reformat_tweet(data._json))
        return True

    def on_error(self, status):
        if status == 420:
            print("rate limit active")
            return False
          
# Make an instance of the class
l = TweetListener()

# Start streaming
stream = tweepy.Stream(auth, l, tweet_mode='extended')
stream.filter(track=lst_hashtags)
