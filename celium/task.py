# -*- coding: utf-8 -*-
"""
A trusted Python task runner.

:copyright: (c) 2016 by Pau Freixes, Arnau Orriols.
:license: BSD, see LICENSE for more details.
"""


class Task(object):
    """ A Task acts as a proxy between the caller and the called for a specifc function.
    """
    def __init__(self, func):
        self._func = func

    def __call__(self, *args, **kwargs):
        # FIXME: nowadays we only support EAGER mode
        return self._func(*args, **kwargs)
