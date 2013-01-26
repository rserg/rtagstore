from redis import Redis
import time
import pickle
from collections import defaultdict
import threading
from  multiprocessing import Queue, Process


class Connection:
    def __init__(self, *args, **kwargs):
        self._host = kwargs.get('host', 'localhost')
        self._password = kwargs.get('password')
        self._port = kwargs.get('port', 6379)
        self._db = kwargs.get('db')
        self._socket_time = kwargs.get('socket_timeout')
        self._connection_pool = kwargs.get('connection_pool')
        self.redis = Redis(host=self._host, port=self._port)
        self._map_connections = {}
        self._map_connections['default'] = self.redis

    def add_connection(self, name, redis_connection):
        if name not in self._map_connections:
            self._map_connections[name] = redis_connection

    def default_connection(self, name):
        if name in self._map_connections:
            self.redis = self._map_connections[name]

    def remove_connection(self, name, replace_name='default'):
        if name in self._map_connections and name != replace_name:
            self._map_connections.pop(name)
            self.redis = self._map_connections[replace_name]



class AbstractRedisStruct:
    def __init__(self, redis_connect, key='mylist'):
        if(not isinstance(redis_connect, Connection)):
            raise Exception("THis is not Connection class")
        self._redis = redis_connect.redis
        self._key = key
        self._keydict = defaultdict()


class RedisList(AbstractRedisStruct):
    def __init__(self, redis_connect, key='mylist'):
        AbstractRedisStruct.__init__(redis_connect, key)
        if(not isinstance(redis_connect, Connection)):
            raise Exception("THis is not Connection class")
        self._redis = redis_connect.redis
        self._key = 'mylist'

    def push(self, key, value):
        self._redis.rpush(key, value)

    def append(self, value):
        self._redis.sadd(self._key, value)

    def defaultlist(self):
        return self._key

    #Return all keys and values
    def getSession(self, session):
        print(self._redis.hgetall(session))

    def tag(self, _key='mylist'):
        if _key != None:
            self._key = _key
        return self._redis.smembers(self._key)

    def __getitem__(self, key):
        pass

    def __setitem__(self, value):
        raise NotImplemented

    def index(self, key, value):
        return self._redis.lindex(key, value)

    def setdefault(self, *args, **kwargs):
        self._key = kwargs.get('key')


'''
redis.tag("foo","bar","foobar")
'''
class TagStore:
    def __init__(self, redis,listname='default'):
        self._redis = redis
        self.listname = listname
    def tag(self, **kwargs):
        self._redis.sadd(self.listname, kwargs)


class Task:
    def __init__(self, key, task, *args, **kwargs):
        self.key = key
        self.task = task
        self.timeout = kwargs.get('timeout', 80)
        self.priority = kwargs.get('priority')
        self.subtask = kwargs.get('subtask',[])
        self.async = kwargs.get('async', True)
        self.args = kwargs.get('args')
        self.kwargs = kwargs.get('kwargs')
        self.status = None

    def addpriority(self, priority):
        self.priority = priority

    def add_subtask(self,subtask):
        self.subtask.append(subtask)

    def statusq(self):
        return self.status

#Basic Task Queue


class RedisQueue(AbstractRedisStruct):
    def __init__(self, redis_connect, key='mylist', maxsize=10000, **kwargs):
        AbstractRedisStruct.__init__(self, redis_connect, key)
        self.maxsize = maxsize
        self.result = None
        self.key = key
        self.priority = kwargs.get('priority')
        self.one_copy = kwargs.get('one_copy', False)

    def put_task(self, value, *args, **kwargs):
        if self.maxsize > self.size():
            key = kwargs.get('key', self.key)
            priority = kwargs.get('priority')
            serialize = pickle.dumps([Task(key, value, args=args, kwargs=kwargs,
                priority = priority)])
            self._redis.lpush(key, serialize)

    def pop_task(self, newprocess=False, **kwargs):
        key = kwargs.get('key', self.key)
        #In the exception case put in queue again
        exp = kwargs.get('backput', False)
        data = self._redis.lpop(key)
        if data != None:
            params = pickle.loads(data)
            params = params[0]
            if newprocess:
                argsqueue = Queue()
                argsqueue.put(params.args)
                argsqueue.put(params.kwargs)
                p = Process(target=params.task, args=(argsqueue,), kwargs=params.kwargs)
                p.start()
            else:
                self.result = params.task(*params.args, **params.kwargs)

    def setkey(self, newkey):
        self.key = newkey

    def set_maxsize(self, maxsize):
        self.maxsize = maxsize

    def result(self):
        return self.result

    def size(self):
        return self._redis.llen(self.key)

    def addtag(self,**kwargs):
        TagStore(self._redis).tag(funtug=kwargs)

    def __eq__(self, name):
        return self.key == name


def test_func(*args, **kwargs):
    print(kwargs)
    return kwargs.get('foobar')

def fun(**kwargs):
    return kwargs