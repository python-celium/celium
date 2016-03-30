# -*- coding: utf-8 -*-
"""
A trusted Python task runner.

:copyright: (c) 2016 by Pau Freixes, Arnau Orriols.
:license: BSD, see LICENSE for more details.
"""
from collections import deque

from celium.commands import AsyncCommand

from celium.broker.commands import PopTask
from celium.broker.commands import PushTask
from celium.broker.commands import CreateQueue


class _Queue(deque):
    """ A :class:`celium.broker.queue._Queue` is the internal container where messages
    destined to the same queue are packed together. This is implemented using a optimized
    container called `deque` that implements few performance improvements from other
    classic containers such as list.

    A queue push messages to the right of the container and the messages stored are consumed
    from the left side getting a FIFO implementation.

    Queues are not instanciated using this class, this class is used as an implementation of the
    common interface  used by the different levels of guarantee queue implementations.

    created automatically by the :func:`celium.broker.queue.QueuesHandler.queue`.
    """

    SPEC = None

    def __init__(self, name):
        self.name = name
        super(_Queue, self).__init__()

    # formalize the I/O methods used
    pop = deque.popleft
    push = deque.append


class _QueueMaster(_Queue):
    """ Used by the diferent levels of guarantees to represent a master queue."""

    SPEC = None

    def __init__(self, name, slaves=2):
        self.slaves = slaves
        super(_QueueMaster, self).__init__(name)

    def slave_name(self, num):
        """ Returns one slave queue name assigned with the number `num`.

        :param num: integer, has to be greater than 0 and lesser or equal than the number of slaves configured.
        :return: string, the name of the slave.
        :raises: ValueError, when the `num` param is not valid.
        """
        if num >= self.slaves:
            raise ValueError("Max slaves {}".format(self.slaves))
        elif num < 0:
            raise ValueError("Min slave number 0")

        return "{}-slave-{}".format(self.name, num)


class _QueueSlave(_Queue):
    """ Used by the diferent levels of guarantees to represent the slave queues"""

    SPEC = None
    pop = None
    push = None


class QueueLevel0Master(_QueueMaster, AsyncCommand):
    """ Level 0 queues have no guarantee in terms of consistence. When a new queue is created, tasks
    are added or tasks are  consumed, the slaves queues related to its are syncronized asynchronously.

    This implementation uses :class:`celium.commands.AsyncCommand` to perform the queue commands
    between the master and slave queues with asynchrony.
    """

    SPEC = 'level0m'

    def __init__(self, name, slaves=2):
        super(QueueLevel0Master, self).__init__(name)
        commands = [self.command(CreateQueue(self.slave_name(x),
                                             self.SPEC)) for x in xrange(0, self.slaves)]

    def push(self, msg):
        """Add a new message to the master and synchronize with the slave istances without wait
        for the response.

        :param msg: str.
        """
        super(QueueLevel0Master, self).push(msg)
        commands = [self.command(PushTask(self.slave_name(x),
                                          msg)) for x in xrange(0, self.slaves)]

    def pop(self):
        """Consume a message from the master and synchronize with the slave istances without wait
        for the response.

        :returns: str, message.
        """
        commands = [self.command(PopTask(self.slave_name(x))) for x in xrange(0, self.slaves)]
        super(QueueLevel0Master, self).pop()


class QueueLevel0Slave(_QueueSlave, AsyncCommand):
    """ Level 0 queues have no guarantee in terms of consistence. A slave instance of a Level 0
    guarantee level acts as syncronized copy that can become eventually inconsistence.

    This implementation uses :class:`celium.commands.AsyncCommand` to perform the queue commands
    between the master and slave queues with asynchrony.
    """

    SPEC = 'level0s'


class QueuesHandler(object):
    """ QueuesHandler tracks all living queues and create new ones using the proper factory depending
    on the type of queue requested.

    The default factory class ued to create new ones is the :class:`celium.broker.queues.QueueLevel0Master`,
    for other types of queues the specific factory has to be used.

    Queues are created automatically if they didnt exist.
    """

    DEFAULT_QUEUE_FACTORY_CLS = QueueLevel0Master

    def __init__(self):
        self.__queues = dict()

    def queue(self, name, create_if_not_exist=True, queue_kwargs=None):
        """ Get a queue named by a specifc `name`, if it does not exist create a new
        ones with no messages.

        :param name: str.
        :param create_if_not_exist: boolean. Default True, create a new one if not exist a queue
                                    by this name.
        :param queue_kwargs: dict. Default None, give a specific set of kw as params to the queue
                             construtor.
        :raises KeyError: When the `create_if_not_exist` if set to False and the queue not exist.
        """
        try:
            return self.__queues[name]
        except KeyError:
            if create_if_not_exist:
                queue_kwargs_ = queue_kwargs or {}
                q = self.DEFAULT_QUEUE_FACTORY_CLS(name, **queue_kwargs_)
                self.__queues[name] = q
                return q
            else:
                raise
