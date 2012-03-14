import unittest
from mock import Mock

class TestRedisDeleteAction(unittest.TestCase):
    def _makeOne(self, key):
        from retools.redistxn import RedisDeleteAction
        return RedisDeleteAction(key)

    def test_finish(self):
        action = self._makeOne('abc')
        pipeline = DummyRedisServer()
        action.finish(pipeline)
        self.assertEqual(pipeline.deleted, ['abc'])

class TestRedisSetAction(unittest.TestCase):
    def _makeOne(self, key, value):
        from retools.redistxn import RedisSetAction
        return RedisSetAction(key, value)

    def test_finish(self):
        action = self._makeOne('abc', '123')
        pipeline = DummyRedisServer()
        action.finish(pipeline)
        self.assertEqual(pipeline.setted, [('abc', '123')])

class TestCacheAPI(unittest.TestCase):
    def _getTargetClass(self):
        from retools.redistxn import CacheAPI
        return CacheAPI

    def _makeOne(self, strict=True):
        redis_server = DummyRedisServer()
        return self._getTargetClass()(redis_server, strict=strict)

    def test_implements_IDataManager(self):
        from zope.interface.verify import verifyClass, verifyObject
        from transaction.interfaces import IDataManager
        verifyClass(IDataManager, self._getTargetClass())
        verifyObject(IDataManager, self._makeOne())

    def test_redis_delete(self):
        inst = self._makeOne()
        inst.redis_delete('abc')
        self.assertEqual(len(inst.redis_actions), 1)
        action = inst.redis_actions[0]
        self.assertEqual(action.__class__.__name__, 'RedisDeleteAction')
        self.assertEqual(action.key, 'abc')

    def test_redis_set(self):
        inst = self._makeOne()
        inst.redis_set('abc', 'value')
        self.assertEqual(len(inst.redis_actions), 1)
        action = inst.redis_actions[0]
        self.assertEqual(action.__class__.__name__, 'RedisSetAction')
        self.assertEqual(action.key, 'abc')
        self.assertEqual(action.value, 'value')

    def test_tpc_vote_strict(self):
        inst = self._makeOne()
        txn = DummyTransaction()
        inst.tpc_vote(txn)

    def test_tpc_vote_not_strict(self):
        inst = self._makeOne(strict=False)
        txn = DummyTransaction()
        inst.tpc_vote(txn)
        self.assertFalse(inst.redis_server.pinged)

    def test_tpc_finish_redis_actions_garden_path(self):
        inst = self._makeOne(strict=False)
        action = DummyAction()
        inst.redis_actions = [action]
        txn = DummyTransaction()
        inst.tpc_finish(txn)
        self.assertTrue(action.finished)

    def test_tpc_finish_redis_actions_exception_strict(self):
        inst = self._makeOne(strict=True)
        logger = DummyLogger()
        inst.logger = logger
        action = DummyAction()
        inst.redis_server.to_raise = ValueError
        inst.redis_actions = [action]
        txn = DummyTransaction()
        self.assertRaises(ValueError, inst.tpc_finish, txn)
        self.assertEqual(logger.messages, ['Redis server is down'])

    def test_tpc_finish_redis_actions_exception_nonstrict(self):
        inst = self._makeOne(strict=False)
        logger = DummyLogger()
        inst.logger = logger
        action = DummyAction()
        inst.redis_server.to_raise = ValueError
        inst.redis_actions = [action]
        txn = DummyTransaction()
        inst.tpc_finish(txn)
        self.assertEqual(logger.messages, ['Redis server is down'])
        self.assertEqual(inst.redis_server.executed, False)

    def test_tpc_abort(self):
        txn = DummyTransaction()
        inst = self._makeOne()
        inst.redis_actions = ['abc']
        inst.tpc_abort(txn)
        self.assertEqual(inst.redis_actions, [])


class DummyTransaction(object):
    pass


class DummyRedisServer(object):
    def __init__(self, to_raise=None):
        self.deleted = []
        self.setted = []
        self.pinged = False
        self.executed = False
        self.to_raise = to_raise

    def delete(self, key):
        self.deleted.append(key)

    def set(self, key, value):
        self.setted.append((key, value))

    def ping(self):
        self.pinged = True

    def execute(self):
        if self.to_raise:
            raise self.to_raise
        self.executed = True

    def pipeline(self):
        return self

    def namespace_key(self, key):
        return key

    def is_alive(self):
        self.pinged = True
        return True


class DummyAction(object):
    def __init__(self, to_raise=None):
        self.finished = False
        self.to_raise = to_raise

    def finish(self, x):
        if self.to_raise:
            raise self.to_raise
        self.finished = x

class DummyLogger(object):
    def __init__(self):
        self.messages = []

    def exception(self, msg):
        self.messages.append(msg)

