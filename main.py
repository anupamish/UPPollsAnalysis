import tweepy
import config
import mysql.connector
from mysql.connector import errorcode


# Tweepy Stream Listener
# This handles all the tweets being streamed from the Twitter Streaming API.
class TweepyStreamListener(tweepy.StreamListener):

    def on_status(self, status):
        print(status.text)

    def on_error(self, status_code):
        if status_code == 420:
            return False


# Connect to MySQL server
def setup_mysql_connection():
    conn = None
    try:
        conn = mysql.connector.connect(**config.MYSQL_CONFIG)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Error: Username or Password is incorrect.")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Error: Bad database name.")
        else:
            print("Error:", err)

    print("Connection to MYSQL db established.")
    return conn


# Authenticate the tweepy module and return the API
def get_tweepy_api():
    auth = tweepy.OAuthHandler(config.CONSUMER_KEY, config.CONSUMER_SECRET)
    auth.set_access_token(config.ACCESS_TOKEN, config.ACCESS_TOKEN_SECRET)
    return tweepy.API(auth)


if __name__ == '__main__':
    # Connect to MySQL database
    db = setup_mysql_connection()

    # Setup the Tweepy API and Authenticaion
    api = get_tweepy_api()
    streamListener = TweepyStreamListener()
    stream = tweepy.Stream(auth=api.auth, listener=streamListener)

    stream.filter(track=['Trump'], async=True)

    # Disconnet from the MySQL database
    db.close()
