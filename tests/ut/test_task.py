# -*- coding: utf-8 -*-
import pytest

from celium.task import Task


class TestTask(object):

    def test_task(self):
        def add(a, b):
            return a + b

        assert Task(add)(2, 2) == 4
