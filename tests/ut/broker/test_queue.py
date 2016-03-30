# -*- coding: utf-8 -*-
import pytest

from mock import patch

from celium.commands import AsyncCommand
from celium.broker.queue import _Queue
from celium.broker.queue import _QueueMaster
from celium.broker.queue import _QueueSlave
from celium.broker.queue import QueuesHandler
from celium.broker.queue import QueueLevel0Master
from celium.broker.queue import QueueLevel0Slave
from celium.broker.commands import PopTask
from celium.broker.commands import PushTask
from celium.broker.commands import CreateQueue


class Test_Queue(object):
    def test_push_pop(self):
        q = _Queue('foo')
        map(q.push, xrange(0, 10))
        assert [q.pop() for _ in xrange(0, 10)] == [x for x in xrange(0, 10)]


class Test_QueueMaster(object):
    def test_init(self):
        q = _QueueMaster('foo')
        q.push(3)
        assert q.slaves == 2
        assert q.pop() == 3

        q = _QueueMaster('foo', slaves=3)
        assert q.slaves == 3

    def test_slave_name(self):
        q = _QueueMaster('foo')
        assert q.slave_name(0) == 'foo-slave-0'
        assert q.slave_name(1) == 'foo-slave-1'

    def test_slave_name_invalid_num(self):
        q = _QueueMaster('foo')
        with pytest.raises(ValueError):
            q.slave_name(-1)

        with pytest.raises(ValueError):
            q.slave_name(2)


class Test_QueueSlave(object):
    def test_pop_push_not_sable(self):
        q = _QueueSlave('foo')

        with pytest.raises(TypeError):
            q.push({})

        with pytest.raises(TypeError):
            q.pop()


@patch.object(AsyncCommand, "command")
class TestQueueLevel0(object):

    def test_create(self, command_patched):
        q = QueueLevel0Master('foo')
        command_patched.assert_called_with(CreateQueue('foo-slave-0', QueueLevel0Master.SPEC))
        command_patched.assert_called_with(CreateQueue('foo-slave-1', QueueLevel0Master.SPEC))

    def test_add_task(self, command_patched):
        msg = {'foo': 'bar'}
        q = QueueLevel0Master('foo')
        q.push(msg)
        command_patched.assert_called_with(PushTask('foo-slave-0', msg))
        command_patched.assert_called_with(PushTask('foo-slave-1', msg))

    def test_consume_task(self, command_patched):
        msg = {'foo': 'bar'}
        q = QueueLevel0Master('foo')
        q.push(msg)
        q.pop()
        command_patched.assert_called_with(PopTask('foo-slave-0'))
        command_patched.assert_called_with(PopTask('foo-slave-1'))


class TestQueuesHandler(object):

    @pytest.fixture(autouse=True)
    def queue_handler(self):
        self.qh = QueuesHandler()

    def test_create(self):
        q = self.qh.queue('foo')

        # default class used to create new queues
        assert isinstance(q, QueueLevel0Master)

    def test_dont_create(self):
        with pytest.raises(KeyError):
            self.qh.queue('foo', create_if_not_exist=False)
