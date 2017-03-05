from textblob import TextBlob

import config
import json
import mysql.connector as msc

get_tweets = "SELECT id, text FROM tweets"

analysed_tweets = []
analysis = {"ave_polarity": 0,
            "max_polarity": -1,
            "min_polarity": 1,
            "ave_subjectivity": 0,
            "max_subjectivity": -1,
            "min_subjectivity": 1,
            "words": {}}


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

        # TODO: Catch table not exists error
        cursor.execute(get_tweets)

        for (tid, text) in cursor:
            tweet = text.replace("\n", " ")
            blob = TextBlob(tweet)
            sentiment = blob.sentiment
            if sentiment.polarity != 0:
                analysed_tweets.append((tid, sentiment.polarity,
                                        sentiment.subjectivity))
                analysis["ave_polarity"] += (sentiment.polarity + 1)
                analysis["ave_subjectivity"] += (sentiment.subjectivity + 1)

                if analysis["max_polarity"] < sentiment.polarity:
                    analysis["max_polarity"] = sentiment.polarity

                if analysis["min_polarity"] > sentiment.polarity:
                    analysis["min_polarity"] = sentiment.polarity

                if analysis["max_subjectivity"] < sentiment.subjectivity:
                    analysis["max_subjectivity"] = sentiment.subjectivity

                if analysis["min_subjectivity"] > sentiment.subjectivity:
                    analysis["min_subjectivity"] = sentiment.subjectivity

        analysis["ave_polarity"] /= len(analysed_tweets)
        analysis["ave_polarity"] -= 1

        analysis["ave_subjectivity"] /= len(analysed_tweets)
        analysis["ave_subjectivity"] -= 1

        print analysis

        with open(config.JSON_FILE, 'w') as fp:
            json.dump(analysis, fp, sort_keys=True, indent=4)

        # Disconnet from the MySQL database
        print "INFO: Closing connection to MYSQL db."
        cursor.close()
        db.close()
