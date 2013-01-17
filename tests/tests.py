import unittest
import rtagstore

class RedisQueueTest(unittest.TestCase):
    def test_connection(self):
        conn =rtagstore.Connection()
    def test_init(self):
        r = rtagstore.RedisQueue(rtagstore.Connection())


if __name__ == '__main__':
    unittest.main()