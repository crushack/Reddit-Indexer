"""MongoDB interfacing module.
"""

from pymongo import MongoClient

import pymongo


DATABASE = 'reddit_parser'

COMMENTS = 'reddit_comments'
SUBMISSIONS = 'reddit_submissions'
WORDS_SUBMISSIONS = 'reddit_words_submissions'
WORDS_COMMENTS = 'reddit_words_comments'

REDDIT = 'reddit__'
COMM = 'comm__'
SUBM = 'subm__'

SEARCH_INDEXES = [[('timestamp', pymongo.ASCENDING)],
                  [('word', pymongo.TEXT), ('timestamp', pymongo.ASCENDING)]]


def __remove_collection(client, collection_name):
    """Removes element in collection.

    Args:
        client: An instance of a pymongo.MongoClient class.
        collection_name: A string representing the name of the collection we
            wish to be emptied.
    """

    client[collection_name].delete_many({})


def __create_query_without_key(from_timestamp, to_timestamp):
    """Creates a querry for a timerange.

    Args:
        from_timestamp: An UNIX timestamp representing the minimum possible
            time of a submission/comment in the querry.
        to_timestamp: An UNIX timestamp representing the maximum possible time
            of a submission/comment in the querry.

    Returns:
        A dictionary representing the query paramenters.
    """

    return {
        'timestamp' : {
            '$gte' : from_timestamp,
            '$lte' : to_timestamp}}


def __create_query_with_key(from_timestamp, to_timestamp, key):
    """Creates a querry for a timerange and a given key.

    Args:
        from_timestamp: An UNIX timestamp representing the minimum possible
            time of a submission/comment in the querry.
        to_timestamp: An UNIX timestamp representing the maximum possible time
            of a submission/comment in the querry.
        key: A string representing a word needed to be contained in the
            title/boddy of each submission/comment.

    Returns:
        A dictionary representing the query paramenters.
    """

    return {
        'timestamp' : {
            '$gte' : from_timestamp,
            '$lte' : to_timestamp},
        '$text' : {
            '$search' : key,
            '$language' : 'none'}}


def __create_query(client, collection, subreddit, from_timestamp, to_timestamp,
                   key=None):
    """Creates a querry for a timerange and possible a given key.

    Args:
        client: An instance of a pymongo.MongoClient class.
        collection: A string representing the type of the collection in which
            we want to insert the elements (submission/comment).
        subreddit: A string representing the name of the subreddit from which we
            want to retrieve the submissions/comments.
        from_timestamp: An UNIX timestamp representing the minimum possible
            time of a submission/comment in the querry.
        to_timestamp: An UNIX timestamp representing the maximum possible time
            of a submission/comment in the querry.
        key: A string representing a word needed to be contained in the
            title/boddy of each submission/comment.

    Returns:
        An interable representing the cursor of the query to the database.
    """

    database = client[DATABASE]
    collection = REDDIT + collection + subreddit

    if key is None:
        query = __create_query_without_key(int(from_timestamp),
                                           int(to_timestamp))
    else:
        query = __create_query_with_key(int(from_timestamp), int(to_timestamp),
                                        key)

    return database[collection].find(query, {'_id':1, 'body': 1})


def init_mongodb(mongo_host, mongo_port, keep_existing=False):
    """Creates MongoDB client and application databases.

    Args:
        mongo_host: A string representing the host server of the database.
        mongo_port: An integer representing the port of the serving mongo db
            on the server.
        keep_existing: A boolean value determining if we are going to keep the
            already existing information in the database.

    Returns:
        An instance of a pymongo.MongoClient class.
    """

    client = MongoClient(mongo_host, mongo_port)
    database = client[DATABASE]

    collection_names = database.collection_names()
    collection_names.remove('system.indexes')
    if not keep_existing:
        for collection_name in collection_names:
            __remove_collection(database, collection_name)

        for collection_name in collection_names:
            database[collection_name].drop_indexes()

    return client


def insert_comments(client, subreddit, comments):
    """Inserts the comments in the database.

    Args:
        client: An instance of a pymongo.MongoClient class.
        subreddit: A string representing the name of the subreddit in which we
            want to write the comments.
        comments: An array of tuples of form (words, comment).
    """

    collection = client[DATABASE]

    to_insert = []
    for words, comment in comments:
        comment['word'] = ' '.join(words)
        to_insert.append(comment)

    collection_name = REDDIT + COMM + subreddit
    for search_index in SEARCH_INDEXES:
        collection[collection_name].ensure_index(search_index,
                                                 default_language='none')
    try:
        collection[collection_name].insert_many(to_insert)
    except pymongo.errors.BulkWriteError as blk:
        print vars(blk)


def insert_submissions(client, subreddit, submissions):
    """Inserts the submissions in the database.

    Args:
        client: An instance of a pymongo.MongoClient class.
        subreddit: A string representing the name of the subreddit in which we
            want to write the submissions.
        submissions: An array of tuples of form (words, submission).
    """

    collection = client[DATABASE]

    to_insert = []
    for words, submission in submissions:
        submission['word'] = words
        to_insert.append(submission)

    collection_name = REDDIT + SUBM + subreddit
    for search_index in SEARCH_INDEXES:
        collection[collection_name].ensure_index(search_index,
                                                 default_language='none')
    collection[collection_name].insert_many(to_insert)


def get_comments(client, subreddit, from_timestamp, to_timestamp, key=None):
    """Retrieves comments from the database.

    Args:
        client: An instance of a pymongo.MongoClient class.
        subreddit: A string representing the name of the subreddit from which we
            want to retrieve the submissions/comments.
        from_timestamp: An UNIX timestamp representing the minimum possible
            time of a submission/comment in the querry.
        to_timestamp: An UNIX timestamp representing the maximum possible time
            of a submission/comment in the querry.
        key: A string representing a word needed to be contained in the
            title/boddy of each submission/comment.

    Returns:
        An interable representing the cursor of the query to the database.
    """

    return __create_query(client, COMM, subreddit,
                          from_timestamp, to_timestamp, key)


def get_submissions(client, subreddit, from_timestamp, to_timestamp, key=None):
    """Retrieves submissions from the database.

    Args:
        client: An instance of a pymongo.MongoClient class.
        subreddit: A string representing the name of the subreddit from which we
            want to retrieve the submissions/comments.
        from_timestamp: An UNIX timestamp representing the minimum possible
            time of a submission/comment in the querry.
        to_timestamp: An UNIX timestamp representing the maximum possible time
            of a submission/comment in the querry.
        key: A string representing a word needed to be contained in the
            title/boddy of each submission/comment.

    Returns:
        An interable representing the cursor of the query to the database.
    """

    return __create_query(client, SUBM, subreddit,
                          from_timestamp, to_timestamp, key)
