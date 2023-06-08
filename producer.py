import tweepy 
# from kafka import KafkaProducer
import logging
import json 
from decouple import config
from KEYS import *

logging.basicConfig(level=logging.INFO)
DEFAULT_THRESHOLD = 10

topic_names = [
    "Opdivo",
    "Keytruda",
    "Tagrisso",
    "Alecensa",
    "Tecentriq",
    "Bevacizumab",
    "Stelara",
    "Zytiga",
    "Lynparza",
    "Lixiana"
]

# def createProducer(topic_names):
#     producer = KafkaProducer(bootstrap_servers="localhost:9092")
#     return producer
    
def authTwitter():
    auth = tweepy.OAuthHandler(consumerKey, consumerSecretKey)
    auth.set_access_token(accessToken, accessSecret)
    api = tweepy.API(auth, wait_on_rate_limit=True)
    return api 

class TweetListener(tweepy.StreamingClient):
    def __init__(self, bearer_token, threshold, topic_names):
        super().__init__(bearer_token)
        self.threshold = threshold
        # self.producer = producer
        self.topic_names = topic_names
        self.tweets = []
    
    # receiving data from API
    def on_data(self, unfiltered_data):
        tweet = json.loads(unfiltered_data)
        if tweet["data"]:
            # we care about the message
            data = {
                "msg": tweet.data.extended_tweet["full_text"].replace(",", "") if hasattr(tweet, "extended_tweet") else tweet.data.text.replace(",", "")
            }
            # the hashtag included in this tweet
            hashtags = tweet["data"]["entities"]["hashtags"]
            print("hashtags: ", hashtags)
            for hashtag in hashtags:
                if hashtag in self.topic_names:
                    self.tweets.append(msg)
                    print(data.msg)
                    # self.producer.send(hashtag, value=json.dumps(data).encode("utf-8"))
        return True
    
    @staticmethod
    def on_error(status_code):
        if status_code == 420:
            return False #this disconnects the stream in on_error method

if __name__ == "__main__":
    stream = TweetListener(bearerToken, DEFAULT_THRESHOLD, topic_names)
    stream.filter(track=topic_names) #filter to get messages with certain wordss