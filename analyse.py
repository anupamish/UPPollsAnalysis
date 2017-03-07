from textblob import TextBlob

import config
import db
import datetime
import json
import mysql.connector as msc


analysed_tweets = []
analysis = {"timestamp": datetime.date.today().isoformat(),
            "ave_pol": 0,
            "min_pol": 1,
            "max_pol": -1,
            "neg_pol": 0,
            "ave_sub": 0,
            "min_sub": 1,
            "max_sub": -1,
            "neg_sub": 0,
            "tweets": 0}


# Analyse the sentiment of the text
def analyse_sentiment(text):
    tweet = TextBlob(text)
    return tweet.sentiment


def compute_polarity(polarity):
    if sentiment.polarity < 0:
        analysis["neg_pol"] += 1
    if analysis["max_pol"] < sentiment.polarity:
        analysis["max_pol"] = sentiment.polarity

    if analysis["min_pol"] > sentiment.polarity:
        analysis["min_pol"] = sentiment.polarity


def compute_subjectivity(subjectivity):
    if sentiment.subjectivity < 0:
        analysis["neg_sub"] += 1

    if analysis["max_sub"] < sentiment.subjectivity:
        analysis["max_sub"] = sentiment.subjectivity

    if analysis["min_sub"] > sentiment.subjectivity:
        analysis["min_sub"] = sentiment.subjectivity


if __name__ == '__main__':
    print "INFO: Start Analysing"

    # Connect to MySQL database
    conn = db.setup_mysql_connection()

    if conn is not None:
        cursor = conn.cursor()

        # TODO: Catch table not exists error
        cursor.execute(db.get_tweets)

        for (tid, text) in cursor:
            analysis["tweets"] += 1
            tweet = text.replace("\n", " ")
            blob = TextBlob(tweet)
            sentiment = blob.sentiment
            if sentiment.polarity != 0:
                analysed_tweets.append((tid, sentiment.polarity,
                                        sentiment.subjectivity))

                analysis["ave_pol"] += (sentiment.polarity + 1)
                analysis["ave_sub"] += (sentiment.subjectivity + 1)

                compute_polarity(sentiment.polarity)
                compute_subjectivity(sentiment.subjectivity)

        analysis["ave_pol"] /= len(analysed_tweets)
        analysis["ave_pol"] -= 1

        analysis["ave_sub"] /= len(analysed_tweets)
        analysis["ave_sub"] -= 1

        print "INFO: Tweet stream analysis."
        print json.dumps(analysis, sort_keys=True, indent=4)

        db.get_db_table(cursor, 'analysis')

        try:
            cursor.execute(db.add_analysis, analysis)
        except msc.Error as err:
            print "WARNING: Stream analysis could not be added to the table."
            print err

        with open(config.JSON_FILE, 'w') as fp:
            json.dump(analysis, fp, sort_keys=True, indent=4)

        # Disconnet from the MySQL database
        print "INFO: Closing connection to MYSQL db."
        conn.commit()
        cursor.close()
        conn.close()
