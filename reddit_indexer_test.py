import unittest
import reddit_indexer

class TestFormatters(unittest.TestCase):
	COMMENT = {
		'body' : 'I\'m a little potato.'
	}

	SUBMISSION = {
		'body' : 'I\'m a little potato.'
	}

	WORDS = sorted(['i\'m', 'a', 'little', 'potato'])

	def test_format_comment(self):
		result = sorted(reddit_indexer.format_comment(TestFormatters.COMMENT))
		self.assertEqual(result, TestFormatters.WORDS)

	def test_format_submission(self):
		result = sorted(
			reddit_indexer.format_submission(TestFormatters.SUBMISSION))
		self.assertEqual(result, TestFormatters.WORDS)


if __name__ == '__main__':
	unittest.main()