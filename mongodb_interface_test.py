import unittest

import mongodb_interface
from mock import Mock, MagicMock

class TestQueries(unittest.TestCase):
    FROM_TIMESTAMP = 1
    TO_TIMESTAMP = 2
    KEY = 'key'

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
            self.client, 'all', TestQueries.FROM_TIMESTAMP,
            TestQueries.TO_TIMESTAMP)

        self.assertEqual(
            query,
            TestQueries.WITH_KEY_QUERRY)

    def test_submissions_query_with_key(self):
        query = mongodb_interface.get_submissions(
            self.client, 'all', TestQueries.FROM_TIMESTAMP,
            TestQueries.TO_TIMESTAMP, TestQueries.KEY)

        self.assertEqual(
            query,
            TestQueries.WITHOUT_KEY_QUERRY)

    def test_comments_query_without_key(self):
        query = mongodb_interface.get_comments(
            self.client, 'all', TestQueries.FROM_TIMESTAMP,
            TestQueries.TO_TIMESTAMP)

        self.assertEqual(
            query,
            TestQueries.WITH_KEY_QUERRY)

    def test_comments_query_with_key(self):
        query = mongodb_interface.get_comments(
            self.client, 'all', TestQueries.FROM_TIMESTAMP,
            TestQueries.TO_TIMESTAMP, TestQueries.KEY)

        self.assertEqual(
            query,
            TestQueries.WITHOUT_KEY_QUERRY)


class TestInsertions(unittest.TestCase):
    pass


if __name__ == '__main__':
    unittest.main()