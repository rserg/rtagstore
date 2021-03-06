from redis import Redis
import time
import pickle
from collections import defaultdict
import threading
from  multiprocessing import Queue, Process
import heapq

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
            raise Exception("This is not Connection class")
        self._redis = redis_connect.redis
        self._key = key
        self._keydict = defaultdict()

    def getKey(self):
        return self._key

    def getKeyDict(self):
        return self._keydict


class RedisList(AbstractRedisStruct):
    def __init__(self, redis_connect, key='mylist'):
        AbstractRedisStruct.__init__(self,redis_connect,key)
    def push(self, key, value):
        self._redis.rpush(key, value)

    def append(self, value):
        self._redis.sadd(self._key, value)

    def defaultlist(self):
        return self._key

    #Return all keys and values
    def getSession(self, session):
        print(self._redis.hgetall(session))

    def tag(self, _key,*args):
        if _key != None:self._key = _key
        for arg in args:
            self._redis.sadd(self._key, arg)
        return self._redis.smembers(self._key)

    def remove_tag(self, _key='mylist',*args):
        if _key !=  None: self._key = _key

    def union_tags(self, key1, key2):
        self._redis.sunion(key1, key2)

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
        self.arguments = kwargs.get('arguments')
        self.kwargs = kwargs.get('kwargs')
        self.status = None

    def addpriority(self, priority):
        self.priority = priority

    def add_subtask(self,subtask):
        self.subtask.append(subtask)

    def statusq(self):
        return self.status

    def load_task(self):
        for times in self.arguments:
            for newargs in times:
                self.task(newargs)

#Basic Task Queue


class RedisQueue(AbstractRedisStruct):
    def __init__(self, redis_connect, key='mylista', maxsize=10000, **kwargs):
        AbstractRedisStruct.__init__(self, redis_connect, key)
        self.maxsize = maxsize
        self.result = None
        self.key = key
        self.keys=set()
        self.priority = kwargs.get('priority')
        self.one_copy = kwargs.get('one_copy', False)


    '''
    optional parameters
    key - defualt key for
    priority - in this case
    arguments - multiply agruments for call one function
             example:
             def foo(bar):
                return bar

             put_task(foo, arguments=[10,25,30])

    '''
    def put_task(self, value, *args, **kwargs):
        key = kwargs.get('key', self.key)
        params = kwargs.get('arguments',[])
        arguments = PriorityArguments(params,**kwargs).valus
        priority = kwargs.get('priority')
        serialize = pickle.dumps([Task(key, value, args=args, kwargs=kwargs,
                priority = priority, arguments=arguments)])

        self._redis.lpush(key, serialize)
        self.keys.add(key)

    def put_single_task(self, name, key, value):
        self.result = self.execute_command('HGET', 'name', self.value)

    '''Exctract stored elements in redis
    newprocess - in this case new element/function will be launched in the new thread
    '''

    def pop_task(self, newprocess=False, **kwargs):
        #Default key
        key = kwargs.get('key', self.key)
        #In the exception case put in queue again
        exp = kwargs.get('backput', False)
        issort = kwargs.get('issort')
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
                self.result = params.load_task()

    '''A function call in the case with few parameters for task
    put_task(test_func, arguments = {'foobar':[40,10,20]})
    pop_tasks()
    return value: None
    '''
    def pop_tasks(self,**kwargs):
        params = self._help_pop_task(key)
        params.load_task()

    '''A function is help function for pop task which return extract
    parameter from redis store
    '''
    def _help_pop_task(self, key):
        data = self._redis.lpop(key)
        if data != None:
            params = pickle.loads(data)
            params = params[0]
            return params

    def pop_single_task(self, name,key,value):
        return self._redis.execute_command('HGET', 'namea', 'default')

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

    def clear(self,**kwargs):
        clear_all = kwargs.get('all', False)
        if clear_all:
            for keys in self.keys: self._redis.delete(keys)
            self.keys.clear()
        else:
            self._redis.delete(kwargs.get('key'))

    def __eq__(self, name):
        return self.key == name

'''params - few arguments in function
optional sortfunc - sorting function for arguments
'''
class PriorityArguments:
    def __init__(self, params, **kwargs):
        self.params = params
        self.issort = kwargs.get('issort')
        self.arguments = kwargs.get('arguments')
        self.sortfunc = kwargs.get('sortfunc', lambda x:x)
        self.valus = self._show_values()
        self.keys = self._show_keys()
        self.summary = self.keys, self.valus

    def _show_keys(self):
        return self.params.keys()

    def _show_values(self):
        newvalues=[]
        for keys in self._show_keys():
            newvalues.append(self._argsort(keys))
        print(newvalues)
        return newvalues

    def _argsort(self, keys):
        if(self.issort): return sorted(self.params[keys], key=self.sortfunc(self.sortfunc))
        else: return self.params[keys]