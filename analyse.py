from textblob import TextBlob
import config
import mysql.connector as msc


get_tweets = "SELECT id, text FROM tweets"


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


# Analyse the sentiment of the text
def analyse_sentiment(text):
    tweet = TextBlob(text)
    return tweet.sentiment


if __name__ == '__main__':
    print "INFO: Start Analysing"

    # Connect to MySQL database
    db = setup_mysql_connection()

    if db is not None:
        cursor = db.cursor()

        cursor.execute(get_tweets)

        for (tid, text) in cursor:
            tweet = text.replace("\n", " ")
            sentiment = analyse_sentiment(tweet)
            if sentiment.polarity != 0:
                print "{}: {}".format(tid, sentiment)

        # Disconnet from the MySQL database
        print "INFO: Closing connection to MYSQL db."
        cursor.close()
        db.close()
