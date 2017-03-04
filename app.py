"""Flask application for retrieving submission and comment from the database.

Example:
    Basic usage::

        $ FLASK_APP=app.py python -m flask run

    Basic usage GET requests::
        /items/?subreddit=all&from=1&to=9999999999
        /items/?subreddit=all&from=1&to=9999999999&key=love
        /items/?subreddit=all&from=1488682000&to=9999999999
        /items/?subreddit=all&from=1488682000&to=9999999999&key=trump

"""

from flask import Flask, request
from mongodb_interface import init_mongodb, get_comments, get_submissions
import json

import time

APP = Flask(__name__)

MONGO_CLIENT = None


@APP.route('/items/', methods=['GET'])
def items():
    """Items getter.

    Returns: A dictionary with 3 attributes: submissions, comments and time
        where submissions is a list of submissions retrieved from the database,
        comments is a list of comments retrieverd from the database and time
        is the total time of retrieval.
    """
    subreddit = request.args['subreddit']
    from_timestamp = request.args['from']
    to_timestamp = request.args['to']
    key = request.args.get('key')

    answer = {"submissions": [], "comments": []}
    timestamp = time.time()
    answer["comments"] = list(get_comments(
        MONGO_CLIENT, subreddit, from_timestamp, to_timestamp, key))
    answer["submissions"] = list(get_submissions(
        MONGO_CLIENT, subreddit, from_timestamp, to_timestamp, key))
    answer["time"] = time.time() - timestamp

    print answer["time"]
    return str(answer)


def connect_mongo():
    """Connects to the mongodb server.
    """
    global MONGO_CLIENT

    with open('config.json') as config_file:
        config_data = json.load(config_file)

    MONGO_CLIENT = init_mongodb(config_data['mongo_host'],
                                config_data['mongo_port'], keep_existing=True)


connect_mongo()
