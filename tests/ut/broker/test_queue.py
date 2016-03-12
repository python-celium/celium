# -*- coding: utf-8 -*-
import pytest

from celium.broker.queue import Queue
from celium.broker.queue import QueuesHandler


class TestQueue(object):

    def test_push_pop(self):
        q = Queue('foo')
        map(q.push, xrange(0, 10))
        assert [q.pop() for _ in xrange(0, 10)] == [x for x in xrange(0, 10)]


class TestQueuesHandler(object):

    @pytest.fixture(autouse=True)
    def queue_handler(self):
        self.qh = QueuesHandler()

    def test_create(self):
        q = self.qh.queue('foo')

    def test_dont_create(self):
        with pytest.raises(KeyError):
            self.qh.queue('foo', create_if_not_exist=False)
