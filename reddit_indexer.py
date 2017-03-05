"""Subreddit submissions and comments retriever.

This script is used to retrieve comments and subscriptions from the subreddits
specified in the config.json file under the name subreddits.


Example:
    Basic usage::

        $ python reddit_indexer.py

    Threads specification::

        $ python reddit_indexer.py --num_threads=5


Attributes:
    submission_limit (int): The maximum number of submission retrieved from
        reddit in a single request (from my tests, it turnes out that it will
        never download more than 100 submissions, but things may change).
    comment_limit (int): The maximum number of comments retrieved from reddit
        in a single request.
    update_time (int): The minimum time (specified in milliseconds) between
        retrievals for any subreddit.
    num_threads (int): The number of threads used in this script (if not
        specified, it will use a thread for every subreddit).
    [no]compress_subreddits (boolean): If the scrupt should delete duplicate
        subreddits.
    [no]erase_database (boolean): If the script should erase the database before
        starting.

Todo:
    pass the lint test :)

"""

from collections import Counter
from itertools import izip
from mongodb_interface import init_mongodb, insert_comments, insert_submissions

import gflags
import json
import logging
import praw
import random
import re
import signal
import sys
import threading
import time


CLOSING = False
CONFIG_DATA = None
MONGO_CLIENT = None

FLAGS = gflags.FLAGS

gflags.DEFINE_integer('submission_limit', 300,
                      'Maximum number of submissions retrieved per subreddit ' +
                      'request.')
gflags.DEFINE_integer('comment_limit', 1000,
                      'Maximum number of comments retrieved per subreddit ' +
                      'request.')
gflags.DEFINE_integer('update_time', 2000,
                      'Time between updates in milliseconds.')
gflags.DEFINE_integer('num_threads', 5,
                      'Number of threads to be used in application.')

gflags.DEFINE_boolean('compress_subreddits', True,
                      'Check if input has the same reddits multiple times.')
gflags.DEFINE_boolean('erase_database', False,
                      'Erase database when starting the script.')


class SubredditRetriever(object):
    """Retrieves comments and submissions from a subreddit.

    Args:
        reddit: An instance of the praw.Reddit class.
        subreddit_name: A string representing the name of the subreddit from
            which the information will be retrieved.
        starting_time: A UNIX timestamp representing the time from which to
            retrieve the strings.

    """

    def __init__(self, reddit, subreddit_name, starting_time):
        self.reddit = reddit
        self.subreddit_name = subreddit_name
        self.timestamp = starting_time

        logging.debug('Subreddit \'%s\' mounted.', subreddit_name)

    def retrieve_submissions(self, timestamp):
        """Retrieves submissions starting from timestamp.

        Note:
            Use the retrieve method.

        Args:
            timestamp: A UNIX timestamp specifying the time from which
                to get the submissions.

        Returns:
            A tuple of (submissions, new_timestamp) where submissions is
            a vector of submissions (with attributes subreddit, body and
            timestamp) and new_timestamp is the timestamp of the latest
            submission.

        """

        new_timestamp = timestamp
        new_submissions = []

        subreddit = self.reddit.subreddit(self.subreddit_name)
        for submission in subreddit.new(limit=FLAGS.submission_limit):
            if timestamp >= int(submission.created):
                continue

            new_timestamp = max(new_timestamp, int(submission.created))
            new_submissions.append({
                'subreddit' : self.subreddit_name,
                'body': submission.title,
                'timestamp': int(submission.created)})

        logging.debug('%d submissions retrieved.', len(new_submissions))

        return new_submissions, new_timestamp

    def retrieve_comments(self, timestamp):
        """Retrieves comments starting from timestamp.

        Note:
            Use the retrieve method.

        Args:
            timestamp: An UNIX timestamp specifying the time from which
                to get the comments.

        Returns:
            A tuple of (submissions, comments, new_timestamp) where comments is
            a vector of comments )with attributes subreddit, body and
            timestamp) and new_timestamp is the timestamp of the latest
            submission.

        """

        new_timestamp = timestamp
        new_comments = []

        subreddit = self.reddit.subreddit(self.subreddit_name)
        for comment in subreddit.comments(limit=FLAGS.comment_limit):
            if timestamp >= int(comment.created):
                continue

            new_timestamp = max(new_timestamp, int(comment.created))
            new_comments.append({
                'subreddit' : self.subreddit_name,
                'body': comment.body,
                'timestamp': int(comment.created)})

        logging.debug('%d comments retrieved.', len(new_comments))

        return new_comments, new_timestamp

    def retrieve(self):
        """Retrieves submissions and comments starting from timestamp.

        Returns:
            A tuple of (comments, new_timestamp) where submissions is
            a vector of submissions (with attributes subreddit, body and
            timestamp), comments is a vector of comments (with attributes
            subreddit, body and timestamp) and new_timestamp is the timestamp of
            the latest submission.

        """

        timestamp = self.timestamp

        (new_submissions, submission_timestamp
        ) = self.retrieve_submissions(timestamp)
        (new_comments, comment_timestamp
        ) = self.retrieve_comments(timestamp)

        new_timestamp = max(submission_timestamp, comment_timestamp)

        self.set_timestamp(new_timestamp)
        return new_submissions, new_comments, new_timestamp

    def set_timestamp(self, timestamp):
        """Sets the timestamp of the latest submission or comment.
        """

        self.timestamp = timestamp


class SubredditThread(threading.Thread):
    """Watches and updates multiple subreddits on a single thread.

    Args:
        reddit: An instance of the praw.Reddit class.
        subreddit_names: A list of strings representing the names of the
            subreddits from which the information will be retrived.
        starting_time: A UNIX timestamp representing the time from which to
            retrieve the strings.

    """

    def __init__(self, reddit, subreddit_names, starting_time):
        super(SubredditThread, self).__init__()

        self.reddit = reddit
        self.starting_time = starting_time
        self.subreddit_retrievers = []

        for subreddit_name in subreddit_names:
            self.add_subreddit(subreddit_name, starting_time)

    def add_subreddit(self, subreddit_name, starting_time):
        """Adds a subreddit to the list of watched subreddits.

        Args:
            subreddit_name: A string representing the name of the subreddit
                that has to be added to the list of watched subreddits.

        """

        self.subreddit_retrievers.append(
            (subreddit_name,
             SubredditRetriever(self.reddit, subreddit_name, starting_time)))

    def run(self):
        """Runs the thread logic.
        """

        while not CLOSING:
            for name, subreddit_retriever in self.subreddit_retrievers:
                (submissions, comments, _) = subreddit_retriever.retrieve()

                submissions_words = [format_submission(submission)
                                     for submission in submissions]
                comments_words = [format_comment(comment)
                                  for comment in comments]

                insert_submissions(MONGO_CLIENT, name,
                                   izip(submissions_words, submissions))
                insert_comments(MONGO_CLIENT, name,
                                izip(comments_words, comments))

            time.sleep(FLAGS.update_time / 1000.0)


def format_comment(comment):
    """Creates a list of the words in comment.

    Args:
        commment: A dictionary representing a comment (with arrtibutes
            subreddit, body, timestamp).

    Returns:
        A list of the lowercase version of the words in the comment.

    """

    return Counter([s.lower() for s in re.findall(r"[\w']+", comment['body'])]
                  ).keys()


def format_submission(submission):
    """Creates a list of the words in submission.

    Args:
        commment: A dictionary representing a submission (with attributes
            subreddit, body, timestamp).

    Returns:
        A list of the lowercase version of the words in the submission.

    """

    return Counter(
        [s.lower() for s in re.findall(r"[\w']+", submission['body'])]).keys()


def sigterm_handler(*_):
    """Activates the closing procedure for all the working threads.
    """

    global CLOSING
    CLOSING = True


def create_simple_threads(reddit, timestamp, subreddits):
    """Creates a thread for every subreddit given.

    Args:
        reddit: An instance of the praw.Reddit class.
        timestamp: An UNIX timestamp specifying the time from which to get the
            comments.
        subreddits: A list of strings representing the names of the
            subreddits from which the information will be retrived.

    Returns:
        A list of SubredditThread instances.
    """

    threads = []
    for subreddit_name in subreddits:
        thread = SubredditThread(reddit, [subreddit_name], timestamp)
        threads.append(thread)
    return threads


def create_multiple_subreddit_threads(reddit, timestamp,
                                      num_threads, subreddits):
    """Evenly distributes subreddits among a number of num_threads threads.

    Args:
        reddit: An instance of the praw.Reddit class.
        timestamp: An UNIX timestamp specifying the time from which to get the
            comments.
        num_threads: An integer representing the number of threads to be
            created.
        subreddits: A list of strings representing the names of the subreddits
            from which the information will be retrived.

    Returns:
        A list of SubredditThread instances.
    """

    random.shuffle(subreddits)

    thread_subreddits = [[] for _ in xrange(num_threads)]
    for i, subreddit in enumerate(subreddits):
        thread_subreddits[i % num_threads].append(subreddit)

    threads = []
    for thread_subreddit in thread_subreddits:
        thread = SubredditThread(reddit, thread_subreddit, timestamp)
        threads.append(thread)

    return threads


def create_threads(reddit, timestamp, num_threads, subreddits):
    """Creates threads depending on the number specified in num_threads.

    Args:
        reddit: An instance of the praw.Reddit class.
        timestamp: An UNIX timestamp specifying the time from which to get the
            comments.
        num_threads: An integer representing the number of threads to be
            created.
        subreddits: A list of strings representing the names of the subreddits
            from which the information will be retrived.

    Returns:
        A list of SubredditThread instances.
    """

    if num_threads and num_threads < len(subreddits):
        return create_multiple_subreddit_threads(reddit, timestamp,
                                                 num_threads, subreddits)
    else:
        return create_simple_threads(reddit, timestamp, subreddits)


def erase_duplicates(li):
    """Erase duplicates in list.

    Args:
        li: A list of elements.

    Returns:
        A list of unique elements created from li.
    """

    return Counter(li).keys()


def main(argv):
    """Program entry point.

    Args:
        argv: A list of arguments that were given to the program.
    """

    global CONFIG_DATA, MONGO_CLIENT

    argv = FLAGS(argv)
    logging.basicConfig(level=logging.DEBUG)

    with open('config.json') as config_file:
        CONFIG_DATA = json.load(config_file)

    MONGO_CLIENT = init_mongodb(CONFIG_DATA['mongo_host'],
                                CONFIG_DATA['mongo_port'],
                                keep_existing=not FLAGS.erase_database)

    reddit = praw.Reddit(client_id=CONFIG_DATA['client_id'],
                         client_secret=CONFIG_DATA['client_secret'],
                         user_agent=CONFIG_DATA['user_agent'])

    if FLAGS.compress_subreddits:
        subreddits = erase_duplicates(CONFIG_DATA['subreddits'])
    else:
        subreddits = CONFIG_DATA['subreddits']

    timestamp = 0 # int(time.time())
    threads = create_threads(reddit, timestamp, FLAGS.num_threads, subreddits)

    for thread in threads:
        thread.start()

    signal.signal(signal.SIGINT, sigterm_handler)

    while not CLOSING:
        time.sleep(5)
        print CLOSING

    for thread in threads:
        thread.join()


if __name__ == '__main__':
    main(sys.argv)
