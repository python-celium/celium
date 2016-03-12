# -*- coding: utf-8 -*-
"""
A trusted Python task runner.

:copyright: (c) 2016 by Pau Freixes, Arnau Orriols.
:license: BSD, see LICENSE for more details.
"""
from collections import deque
from weakref import WeakValueDictionary


class Queue(deque):
    """ A :class:`celium.broker.queue.Queue` is the internal container where messages
    destined to the same queue are packed together. This is implemented using a optimized
    container called `deque` that implements few performance improvements from other
    classic containers such as list.

    A queue push messages to the right of the container and the messages stored are consumed
    from the left side getting a LIFO implementation.

    Queues are created automatically by the :func:`celium.broker.queue.QueuesHandler.queue`.
    """

    def __init__(self, name):
        self.name = name
        super(Queue, self).__init__()

    # formalize the I/O methods used
    pop = deque.popleft
    push = deque.append


class QueuesHandler(object):
    """ A :class:`celium.broker.queue.Queue` is used to handle a set of queues, to create
    new ones or just get the reference to one queue.
    """

    def __init__(self):
        self.__queues = dict()

    def queue(self, name, create_if_not_exist=True):
        """ Get a queue named by a specifc `name`, if it does not exist create a new
        ones with no messages.

        :param name: str.
        :param create_if_not_exist: boolean. Default True, create a new one if not exist a queue
                                    by this name.
        :raises KeyError: When the `create_if_not_exist` if set to False and the queue not exist.
        """
        try:
            return self.__queues[name]
        except KeyError:
            if create_if_not_exist:
                q = Queue(name)
                self.__queues[name] = q
                return q
            else:
                raise
