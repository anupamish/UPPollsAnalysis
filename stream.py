import config
import datetime
import mysql.connector as msc
import tweepy


"""
MySQL Table Design
-----------------------------------------------
| id | timestamp | text | user_id | user_name |
-----------------------------------------------
"""

TABLES = {}
TABLES['tweets'] = (
    "CREATE TABLE `tweets` ("
    "   `id` BIGINT NOT NULL UNIQUE,"
    "   `timestamp` DATE NOT NULL,"
    "   `text` VARCHAR(140) NOT NULL,"
    "   `user_id` BIGINT NOT NULL,"
    "   `user_name` VARCHAR(50) NOT NULL,"
    "   PRIMARY KEY (`id`)"
    ")")

add_status = ("INSERT INTO tweets"
              "(id, timestamp, text, user_id, user_name) "
              "VALUES "
              "(%(id)s, %(timestamp)s, %(text)s, %(user_id)s, %(user_name)s)")


# Connect to MySQL server
def setup_mysql_connection():
    conn = None

    try:
        conn = msc.connect(**config.MYSQL_CONFIG)
    except msc.Error as err:
        if err.errno == msc.errorcode.ER_ACCESS_DENIED_ERROR:
            print "ERROR: Username or Password is incorrect."
        elif err.errno == msc.errorcode.ER_BAD_DB_ERROR:
            print "ERROR: Bad database name."
        else:
            print "ERROR: {}".format(err)

    if conn is not None:
        print "INFO: Established connection to MYSQL Server."

    return conn


# Return the requested table in the db. Create a table if it doesn't exist.
def get_db_table(cursor, name):
    try:
        print "INFO: Creating table {}".format(name)
        cursor.execute(TABLES[name])
    except msc.Error as err:
        if err.errno == msc.errorcode.ER_TABLE_EXISTS_ERROR:
            print "INFO: Already Exists."
        else:
            print err.msg
    else:
        print "INFO: OK"


# Strip the tweet to retain the meaningful fields
def skim_tweet(tweet):
    return {"id": tweet.id,
            "timestamp": tweet.created_at.date(),
            "text": tweet.text,
            "user_id": tweet.user.id,
            "user_name": tweet.user.name}


# Add tweet to the database
def add_tweet(cursor, tweet):
    try:
        cursor.execute(add_status, tweet)
    except:
        return False
    else:
        return True


# Tweepy Stream Listener
# This handles all the tweets being streamed from the Twitter Streaming API.
class TweepyStreamListener(tweepy.StreamListener):

    def __init__(self, api, cursor):
        self.api = api
        self.cursor = cursor
        self.expire = datetime.datetime.now() + datetime.timedelta(minutes=10)
        self.cnt = 0
        self.success = 0

    def on_status(self, status):
        if self.expire > datetime.datetime.now():
            tweet = skim_tweet(status)

            if add_tweet(self.cursor, tweet):
                self.success += 1
            self.cnt += 1

            return True
        else:
            print "INFO: {}/{} successful insertions".format(self.success,
                                                             self.cnt)
            return False

    def on_error(self, status_code):
        if status_code == 420:
            return False


# Authenticate the tweepy module and return the API
def get_tweepy_api():
    auth = tweepy.OAuthHandler(config.CONSUMER_KEY, config.CONSUMER_SECRET)
    auth.set_access_token(config.ACCESS_TOKEN, config.ACCESS_TOKEN_SECRET)
    return tweepy.API(auth)


if __name__ == '__main__':
    # Connect to MySQL database
    db = setup_mysql_connection()

    if db is not None:
        cursor = db.cursor()
        get_db_table(cursor, 'tweets')

        # Set character set
        cursor.execute('SET NAMES utf8mb4')
        cursor.execute("SET CHARACTER SET utf8mb4")
        cursor.execute("SET character_set_connection=utf8mb4")

        # Setup the Tweepy API and Authenticaion
        api = get_tweepy_api()
        streamListener = TweepyStreamListener(api, cursor)
        stream = tweepy.Stream(auth=api.auth, listener=streamListener)

        stream.filter(track=['Trump'])

        # Disconnet from the MySQL database
        print "INFO: Closing connection to MYSQL db."
        db.commit()
        cursor.close()
        db.close()