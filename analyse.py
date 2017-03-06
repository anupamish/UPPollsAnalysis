from textblob import TextBlob

import config
import datetime
import json
import mysql.connector as msc


TABLES = {}
TABLES['analysis'] = (
    "CREATE TABLE `analysis` ("
    "   `id` INT NOT NULL AUTO_INCREMENT,"
    "   `timestamp` DATE NOT NULL,"
    "   `ave_pol` FLOAT NOT NULL,"
    "   `min_pol` FLOAT NOT NULL,"
    "   `max_pol` FLOAT NOT NULL,"
    "   `ave_sub` FLOAT NOT NULL,"
    "   `min_sub` FLOAT NOT NULL,"
    "   `max_sub` FLOAT NOT NULL,"
    "   `tweets` BIGINT NOT NULL,"
    "   PRIMARY KEY (`id`)"
    ")")

add_analysis = ("INSERT INTO analysis"
                "(timestamp, ave_pol, min_pol, max_pol,"
                " ave_sub, min_sub, max_sub, tweets) "
                "VALUES "
                "(%(timestamp)s, %(ave_pol)s, %(min_pol)s, %(max_pol)s,"
                " %(ave_sub)s, %(min_sub)s, %(max_sub)s, %(tweets)s)")

get_tweets = "SELECT id, text FROM tweets"

analysed_tweets = []
analysis = {"timestamp": datetime.date.today().isoformat(),
            "ave_pol": 0,
            "min_pol": 1,
            "max_pol": -1,
            "ave_sub": 0,
            "min_sub": 1,
            "max_sub": -1,
            "tweets": 0}


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
            analysis["tweets"] += 1
            tweet = text.replace("\n", " ")
            blob = TextBlob(tweet)
            sentiment = blob.sentiment
            if sentiment.polarity != 0:
                analysed_tweets.append((tid, sentiment.polarity,
                                        sentiment.subjectivity))
                analysis["ave_pol"] += (sentiment.polarity + 1)
                analysis["ave_sub"] += (sentiment.subjectivity + 1)

                if analysis["max_pol"] < sentiment.polarity:
                    analysis["max_pol"] = sentiment.polarity

                if analysis["min_pol"] > sentiment.polarity:
                    analysis["min_pol"] = sentiment.polarity

                if analysis["max_sub"] < sentiment.subjectivity:
                    analysis["max_sub"] = sentiment.subjectivity

                if analysis["min_sub"] > sentiment.subjectivity:
                    analysis["min_sub"] = sentiment.subjectivity

        analysis["ave_pol"] /= len(analysed_tweets)
        analysis["ave_pol"] -= 1

        analysis["ave_sub"] /= len(analysed_tweets)
        analysis["ave_sub"] -= 1

        print "INFO: Tweet stream analysis."
        print json.dumps(analysis, sort_keys=True, indent=4)

        get_db_table(cursor, 'analysis')

        try:
            cursor.execute(add_analysis, analysis)
        except msc.Error as err:
            print "WARNING: Stream analysis could not be added to the table."
            print err

        with open(config.JSON_FILE, 'w') as fp:
            json.dump(analysis, fp, sort_keys=True, indent=4)

        # Disconnet from the MySQL database
        print "INFO: Closing connection to MYSQL db."
        db.commit()
        cursor.close()
        db.close()
