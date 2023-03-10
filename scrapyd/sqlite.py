import json
import sqlite3
from datetime import datetime
import redis

try:
    from collections.abc import MutableMapping
except ImportError:
    from collections import MutableMapping


class JsonSqliteDict(MutableMapping):
    """SQLite-backed dictionary"""

    def __init__(self, database=None, table="dict"):
        database_host = "redisdocker"
        database_port = 6379
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

        self.queue = "scrapyd-redis.queue.{}.{}".format(database, table)

    def __getitem__(self, key):
        self.decode(self.conn.get(key))

    def __setitem__(self, key, value):
        self.conn.set(key, self.encode(value))

    def __delitem__(self, key):
        self.conn.delete(key)

    def __len__(self):
        return len(self.conn.keys("*"))

    def __iter__(self):
        for k in self.iterkeys():
            yield k

    def iterkeys(self):
        for k in self.conn.keys("*"):
            yield k

    def keys(self):
        return list(self.conn.keys("*"))

    def itervalues(self):
        for k in self.conn.keys("*"):
            yield self.decode(self.conn.get(k))

    def values(self):
        return list(self.itervalues())

    def iteritems(self):
        for k in self.conn.keys("*"):
            yield k, self.decode(self.conn.get(k))

    def items(self):
        return list(self.iteritems())

    def encode(self, obj):
        return json.dumps(obj)

    def decode(self, text):
        return json.loads(text)


class JsonSqlitePriorityQueue(object):
    """SQLite priority queue. It relies on SQLite concurrency support for
    providing atomic inter-process operations.
    """

    def __init__(self, database=None, table="queue"):
        self.database = database or ":memory:"
        self.table = table
        # about check_same_thread: http://twistedmatrix.com/trac/ticket/4040
        self.conn = sqlite3.connect(self.database, check_same_thread=False)
        q = (
            "create table if not exists %s (id integer primary key, "
            "priority real key, message blob)" % table
        )
        self.conn.execute(q)

    def put(self, message, priority=0.0):
        args = (priority, self.encode(message))
        q = "insert into %s (priority, message) values (?,?)" % self.table
        self.conn.execute(q, args)
        self.conn.commit()

    def pop(self):
        q = "select id, message from %s order by priority desc limit 1" % self.table
        idmsg = self.conn.execute(q).fetchone()
        if idmsg is None:
            return
        id, msg = idmsg
        q = "delete from %s where id=?" % self.table
        c = self.conn.execute(q, (id,))
        if not c.rowcount:  # record vanished, so let's try again
            self.conn.rollback()
            return self.pop()
        self.conn.commit()
        return self.decode(msg)

    def remove(self, func):
        q = "select id, message from %s" % self.table
        n = 0
        for id, msg in self.conn.execute(q):
            if func(self.decode(msg)):
                q = "delete from %s where id=?" % self.table
                c = self.conn.execute(q, (id,))
                if not c.rowcount:  # record vanished, so let's try again
                    self.conn.rollback()
                    return self.remove(func)
                n += 1
        self.conn.commit()
        return n

    def clear(self):
        self.conn.execute("delete from %s" % self.table)
        self.conn.commit()

    def __len__(self):
        q = "select count(*) from %s" % self.table
        return self.conn.execute(q).fetchone()[0]

    def __iter__(self):
        q = "select message, priority from %s order by priority desc" % self.table
        return ((self.decode(x), y) for x, y in self.conn.execute(q))

    def encode(self, obj):
        return sqlite3.Binary(json.dumps(obj).encode("ascii"))

    def decode(self, text):
        return json.loads(bytes(text).decode("ascii"))


class SqliteFinishedJobs(object):
    """SQLite finished jobs."""

    def __init__(self, database=None, table="finished_jobs"):
        pass

    def add(self, job):
        pass

    def clear(self, finished_to_keep=None):
        pass

    def __len__(self):
        return 0

    def __iter__(self):
        return []
