from zope.interface import implementer
import redis
import json
from scrapyd.interfaces import ISpiderQueue
from scrapyd.sqlite import JsonSqlitePriorityQueue
from scrapyd.utils import sqlite_connection_string


@implementer(ISpiderQueue)
class SqliteSpiderQueue(object):
    def __init__(self, config, project, table="spider_queue"):
        # self.q = JsonSqlitePriorityQueue(sqlite_connection_string(config, project), table)

        # database_name = config.get('redis_db', project)
        database_host = config.get("redis_host", "localhost")
        database_port = config.getint("redis_port", 6379)
        # database_user = self.get_optional_config(config, 'redis_user')
        # database_pwd = self.get_optional_config(config, 'redis_pass')

        self.conn = redis.StrictRedis(
            host=database_host,
            port=int(database_port),
            charset="utf-8",
            decode_responses=True,
            # db=int(database_name),
            # password=database_pwd
        )

        self.queue = "scrapyd-redis.queue.{}.{}".format(project, table)

    def add(self, name, priority=0.0, **spider_args):
        d = spider_args.copy()
        d["name"] = name
        # self.q.put(d, priority=priority)
        self.conn.zincrby(self.queue, priority, self.encode(d))

    def pop(self):
        # return self.q.pop()
        try:
            _item = self.conn.zrevrange(self.queue, 0, 0)[0]
            if self.conn.zrem(self.queue, _item) == 1:
                return self.decode(_item)
        except IndexError:
            pass

    def count(self):
        return self.conn.zcard(self.queue)

    def list(self):
        return (
            (self.decode(obj[0]), obj[1])
            for obj in self.conn.zrange(
                name=self.queue, start=0, end=-1, withscores=True
            )
        )

    def remove(self, func):
        count = 0
        for msg in self.conn.zrange(self.queue, 0, -1):
            if func(self.decode(msg)):
                self.conn.zrem(self.queue, msg)
                count += 1
        return count

    def clear(self):
        self.conn.delete(self.queue)

    def encode(self, obj):
        return json.dumps(obj)

    def decode(self, text):
        return json.loads(text)
