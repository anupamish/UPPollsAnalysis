import config
import json
import mysql.connector as msc
import tweepy


# Tweepy Stream Listener
# This handles all the tweets being streamed from the Twitter Streaming API.
class TweepyStreamListener(tweepy.StreamListener):

    def on_status(self, status):
        # TODO: Insert this tweet in to the DB
        print(status.text)

    def on_data(self, data):
        try:
            f = open(config.FILE_NAME, 'a')
            tweet = json.loads(data)
            tweet_str = "{},{},\"{}\",{},{}\n".format(
                tweet['id'],
                tweet['created_at'],
                tweet['text'].encode('utf8'),
                tweet['user']['id'],
                tweet['user']['name'].encode('utf8'))
            f.write(tweet_str)
            f.close()
        except:
            print(data)

    def on_error(self, status_code):
        if status_code == 420:
            return False


# Add tweet to the database
def add_tweet(tweet):
    pass


# Strip the tweet to retian the meaningful fields
def skim_tweet(tweet):
    pass


# Authenticate the tweepy module and return the API
def get_tweepy_api():
    auth = tweepy.OAuthHandler(config.CONSUMER_KEY, config.CONSUMER_SECRET)
    auth.set_access_token(config.ACCESS_TOKEN, config.ACCESS_TOKEN_SECRET)
    return tweepy.API(auth)


"""
MySQL Table Design
-----------------------------------------------
| id | timestamp | text | user_id | user_name |
-----------------------------------------------
"""

TABLES = {}
TABLES['tweets'] = (
    "CREATE TABLE `tweets` ("
    "   `id` BIGINT NOT NULL"
    "   `timestamp` DATE NOT NULL"
    "   `text` VARCHAR(140) NOT NULL"
    "   `user_id` BIGINT NOT NULL"
    "   `user_name` VARCHAR(15) NOT NULL"
    ")")


# Connect to MySQL server
def setup_mysql_connection():
    conn = None
    try:
        conn = msc.connect(**config.MYSQL_CONFIG)
    except msc.Error as err:
        if err.errno == msc.errorcode.ER_ACCESS_DENIED_ERROR:
            print "Error: Username or Password is incorrect."
        elif err.errno == msc.errorcode.ER_BAD_DB_ERROR:
            print "Error: Bad database name."
        else:
            print "Error", err

    if conn is not None:
        print "Connection to MYSQL db established."

    return conn


# Return the requested table in the db. Create a table if it doesn't exist.
def get_db_table(cursor, name):
    try:
        print "Creating table", name, ":"
        cursor.execute(TABLES[name])
    except msc.Error as err:
        if err.errno == msc.errorcode.ER_TABLE_EXISTS_ERROR:
            print "Already Exists."
        else:
            print(err.msg)
    else:
        print("OK")
    pass


if __name__ == '__main__':
    # Connect to MySQL database
    db = setup_mysql_connection()

    # Setup the Tweepy API and Authenticaion
    api = get_tweepy_api()
    streamListener = TweepyStreamListener()
    stream = tweepy.Stream(auth=api.auth, listener=streamListener)

    stream.filter(track=['Trump'])

    # Disconnet from the MySQL database
    if db is not None:
        print "Closing connection to MYSQL db."
        db.close()
