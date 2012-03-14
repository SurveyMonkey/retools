import transaction
import logging

from zope.interface import implements
from transaction.interfaces import IDataManager

logger = logging.getLogger('__name__')


class RedisDeleteAction(object):
    def __init__(self, key):
        self.key = key

    def finish(self, pipeline):
        pipeline.delete(self.key)


class RedisSetAction(object):
    def __init__(self, key, value):
        self.key = key
        self.value = value

    def finish(self, pipeline):
        pipeline.set(self.key, self.value)


class CacheAPI(object):
    implements(IDataManager)
    transaction_manager = transaction.manager

    def __init__(self, redis_server, strict=True):
        self.redis_server = redis_server
        self.strict = strict
        self.redis_actions = []
        self.logger = logger # for test overrides

    def redis_delete(self, key):
        new_key = self.redis_server.namespace_key(key).replace(' ', '_')
        self.redis_actions.append(RedisDeleteAction(new_key))

    def redis_set(self, key, value):
        new_key = self.redis_server.namespace_key(key).replace(' ', '_')
        self.redis_actions.append(RedisSetAction(new_key, value))

    def commit(self, transaction):
        pass # pragma: no cover

    def abort(self, transaction):
        pass # pragma: no cover

    def sortKey(self):
        return '~~cacheapi:%d' % id(self)

    def abort_sub(self, transaction):
        pass  # pragma: no cover

    commit_sub = abort_sub

    def beforeCompletion(self, transaction):
        pass  # pragma no cover

    afterCompletion = beforeCompletion

    def tpc_begin(self, transaction, subtransaction=False):
        assert not subtransaction # pragma: no cover

    def tpc_vote(self, transaction):
        if self.strict:
            self.redis_server.is_alive()

    def tpc_finish(self, transaction):
        pipeline = self.redis_server.pipeline()
        for redis_action in self.redis_actions:
            redis_action.finish(pipeline)
        try:
            pipeline.execute()
        except:
            self.logger.exception('Redis server is down')
            if self.strict:
                raise

    def tpc_abort(self, transaction):
        self.redis_actions = []
