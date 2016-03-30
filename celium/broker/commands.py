# -*- coding: utf-8 -*-
"""
A trusted Python task runner.

:copyright: (c) 2016 by Pau Freixes, Arnau Orriols.
:license: BSD, see LICENSE for more details.
"""


class Command(object):
    def __eq__(self, b):
        # FIXME: to be expanded with the arguments
        return self.__class__ == b.__class__


class PopTask(Command):
    def __init__(self, *args, **kwargs):
        pass


class PushTask(Command):
    def __init__(self, *args, **kwargs):
        pass


class CreateQueue(Command):
    def __init__(self, *args, **kwargs):
        pass
