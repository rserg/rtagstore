from redis import Redis
import time
import pickle
from collections import defaultdict

class Connection:
	def __init__(self,*args, **kwargs):
		self._host = kwargs.get('host', 'localhost')
		self._password = kwargs.get('password')
		self._port = kwargs.get('port', 6379)
		self._db = kwargs.get('db')
		self._socket_time = kwargs.get('socket_timeout')
		self._connection_pool = kwargs.get('connection_pool')
		self.redis = Redis(host = self._host, port = self._port)
		self._map_connections={}
		self._map_connections['default'] = self.redis

	def add_connection(self, name, redis_connection):
		if name not in self._map_connections:
			self._map_connections[name] = redis_connection

	def default_connection(self, name):
		if name in self._map_connections:
			self.redis = self._map_connections[name]

	def remove_connection(self, name):
		if name in self._map_connections and name != 'default':
			self._map_connections.pop(name)
			self.redis = self._map_connections['default']


class AbstractRedisStruct:
	def __init__(self, redis_connect,key='mylist'):
		if(not isinstance(redis_connect, Connection)):
			raise Exception("THis is not Connection class")
		self._redis = redis_connect.redis
		self._key=key
		self._keydict = defaultdict()

class RedisList(AbstractRedisStruct):
	def __init__(self,redis_connect, key='mylist'):
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

	def tag(self, _key = 'mylist'):
		if _key !=  None: self._key = _key
		return self._redis.smembers(self._key)

	def __getitem__(self, key):
		pass

	def __setitem__(self, value):
		raise NotImplemented

	def index(self, key, value):
		return self._redis.lindex(key, value)

	def setdefault(self, *args, **kwargs):
		self._key = kwargs.get('key')


#Basic Task Queue
class RedisQueue(AbstractRedisStruct):
	def __init__(self, redis_connect, key='mylist', maxsize=10000,**kwargs):
		AbstractRedisStruct.__init__(self, redis_connect, key)
		self.maxsize = maxsize
		self.result = None
		self.key = key
		self.one_copy = kwargs.get('one_copy', False)
	def put_task(self, key, value, *args,**kwargs):
		serialize = pickle.dumps([value, args, kwargs])
		self._redis.lpush(key, serialize)
		self.results = []

	def pop_task(self, key):
		data = self._redis.lpop(key)
		if data != None:
			function, args, kwargs = pickle.loads(data)
			self.result = function(*args, **kwargs)
			self.results.append(self.result)

	def set_maxsize(self, maxsize):
		self.maxsize = maxsize

	def result(self):
		return self.result

	def save(self):
		pass

	def __eq__(self,name):
		return self.key == name