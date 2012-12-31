from redis import Redis

class Connection:
	def __init__(self,*args, **kwargs):
		self._host = kwargs.get('host', 'localhost')
		self._password = kwargs.get('password')
		self._port = kwargs.get('port', 6379)
		self._db = kwargs.get('db')
		self._socket_time = kwargs.get('socket_timeout')
		self._connection_pool = kwargs.get('connection_pool')
		self.redis = Redis(host = self._host, port = self._port)


class RedisList:
	def __init__(self,redis_connect):
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
		return self._redis.smembers(self._key))

	def __getitem__(self, key):
		pass

	def __setitem__(self, value):
		raise NotImplemented

	def index(self, key, value):
		return self._redis.lindex(key, value)

	def setdefault(self, *args, **kwargs):
		self._key = kwargs.get('key')