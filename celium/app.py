# -*- coding: utf-8 -*-
"""
A trusted Python task runner.

:copyright: (c) 2016 by Pau Freixes, Arnau Orriols.
:license: BSD, see LICENSE for more details.
"""
from functools import wraps

from celium.task import Task


class App(object):
    """ Celium :class:`celium.app.App` instances are the main entry point to publish new
    runnable tasks. Each app is responsable for as many tasks as many of them have been
    related, usually via a decorator.

    Both the caller and the runner use the same interface to couple the interface.
    """

    def __init__(self, *args, **kwargs):
        self._tasks = {}

    def task(self, func):
        """ Add a new task to the current app.

        :param func: callable, entry point of the task.
        """
        t = Task(func)
        self._tasks[func.__name__] = t

    def __getattr__(self, attr_name):
        try:
            return self._tasks[attr_name]
        except KeyError:
            return super(App, self).__getattr__(attr_name)
