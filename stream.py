import config
import datetime
import db
import tweepy


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
        cursor.execute(db.add_status, tweet)
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
        try:
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
        except KeyboardInterrupt:
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
    conn = db.setup_mysql_connection()

    if conn is not None:
        cursor = conn.cursor()
        db.get_db_table(cursor, 'tweets')

        # Set character set
        cursor.execute('SET NAMES utf8mb4')
        cursor.execute("SET CHARACTER SET utf8mb4")
        cursor.execute("SET character_set_connection=utf8mb4")

        # Setup the Tweepy API and Authenticaion
        api = get_tweepy_api()
        streamListener = TweepyStreamListener(api, cursor)
        stream = tweepy.Stream(auth=api.auth, listener=streamListener)

        stream.filter(track=['#upelections2017,'
                             '@yadavakhilesh,'
                             '#bjp, #sp, #bsp,'
                             '#modi, #mayawati, #upelections'])

        # Disconnet from the MySQL database
        print "INFO: Closing connection to MYSQL db."
        conn.commit()
        cursor.close()
        conn.close()
