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
    def test_tags(self):
        lis = RedisList(Connection())
        lis.tag('mylist', 'nice', 'good')

    def test_union(self):
        lis = RedisList(Connection())
        lis.union_tags('mylist','mylist')


if __name__ == '__main__':
    unittest.main()