import config
import mysql.connector as msc


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
    "   `neg_pol` BIGINT NOT NULL,"
    "   `neg_sub` BIGINT NOT NULL,"
    "   `tweets` BIGINT NOT NULL,"
    "   PRIMARY KEY (`id`)"
    ")")

add_status = ("INSERT INTO tweets"
              "(id, timestamp, text, user_id, user_name) "
              "VALUES "
              "(%(id)s, %(timestamp)s, %(text)s, %(user_id)s, %(user_name)s)")


add_analysis = ("INSERT INTO analysis"
                "(timestamp, ave_pol, min_pol, max_pol,"
                " ave_sub, min_sub, max_sub, neg_pol, neg_sub, tweets) "
                "VALUES "
                "(%(timestamp)s, %(ave_pol)s, %(min_pol)s, %(max_pol)s,"
                " %(ave_sub)s, %(min_sub)s, %(max_sub)s, %(neg_pol)s,"
                " %(neg_sub)s, %(tweets)s)")

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
