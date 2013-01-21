import unittest
import rtagstore

class RedisQueueTest(unittest.TestCase):
    def test_connection(self):
        conn =rtagstore.Connection()
    def test_init(self):
        r = rtagstore.RedisQueue(rtagstore.Connection())
    def test_pop(self):
        def fun_func(param):
            return param * 2
        r = rtagstore.RedisQueue(rtagstore.Connection())
        r.put(fun_func)


if __name__ == '__main__':
    unittest.main()