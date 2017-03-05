import unittest

import mongodb_interface
from mock import call, Mock, MagicMock

class TestQueries(unittest.TestCase):
    FROM_TIMESTAMP = 1
    TO_TIMESTAMP = 2
    KEY = 'key'
    SUBREDDIT = 'all'

    WITH_KEY_QUERRY = {
        'timestamp' : {
            '$gte': FROM_TIMESTAMP,
            '$lte': TO_TIMESTAMP}}

    WITHOUT_KEY_QUERRY = {
        'timestamp' : {
            '$gte' : FROM_TIMESTAMP,
            '$lte' : TO_TIMESTAMP},
         '$text' : {
            '$search' : KEY,
            '$language' : 'none'}}

    def setUp(self):
        collection = MagicMock()
        collection.find = lambda a, b: a

        database = MagicMock()
        database.__getitem__.return_value = collection

        client = MagicMock()
        client.__getitem__.return_value = database

        self.client = client

    def test_submissions_query_without_key(self):
        query = mongodb_interface.get_submissions(
            self.client, TestQueries.SUBREDDIT, TestQueries.FROM_TIMESTAMP,
            TestQueries.TO_TIMESTAMP)

        self.assertEqual(
            query,
            TestQueries.WITH_KEY_QUERRY)

    def test_submissions_query_with_key(self):
        query = mongodb_interface.get_submissions(
            self.client, TestQueries.SUBREDDIT, TestQueries.FROM_TIMESTAMP,
            TestQueries.TO_TIMESTAMP, TestQueries.KEY)

        self.assertEqual(
            query,
            TestQueries.WITHOUT_KEY_QUERRY)

    def test_comments_query_without_key(self):
        query = mongodb_interface.get_comments(
            self.client, TestQueries.SUBREDDIT, TestQueries.FROM_TIMESTAMP,
            TestQueries.TO_TIMESTAMP)

        self.assertEqual(
            query,
            TestQueries.WITH_KEY_QUERRY)

    def test_comments_query_with_key(self):
        query = mongodb_interface.get_comments(
            self.client, TestQueries.SUBREDDIT, TestQueries.FROM_TIMESTAMP,
            TestQueries.TO_TIMESTAMP, TestQueries.KEY)

        self.assertEqual(
            query,
            TestQueries.WITHOUT_KEY_QUERRY)


class TestInsertionIndexes(unittest.TestCase):
    SUBREDDIT = 'all'

    def setUp(self):
        ensure_index = Mock()

        collection = MagicMock()
        collection.ensure_index = ensure_index

        database = MagicMock()
        database.__getitem__.return_value = collection

        client = MagicMock()
        client.__getitem__.return_value = database

        calls = []
        for search_index in mongodb_interface.SEARCH_INDEXES:
            calls.append(call(search_index, default_language='none'))

        self.calls = calls
        self.client = client
        self.ensure_index = ensure_index

    def test_insert_submissions_indexes(self):
        mongodb_interface.insert_submissions(
            self.client, TestInsertionIndexes.SUBREDDIT, [])
        self.ensure_index.assert_has_calls(self.calls)

    def test_insert_comments_indexes(self):
        mongodb_interface.insert_comments(
            self.client, TestInsertionIndexes.SUBREDDIT, [])
        self.ensure_index.assert_has_calls(self.calls)


if __name__ == '__main__':
    unittest.main()