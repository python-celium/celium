# -*- coding: utf-8 -*-
import pytest

from celium import App


@pytest.fixture
def app():
    return App()


class TestApp(object):

    def test_task(self, app):
        def add(a, b):
            return a + b

        app.task(add)
        assert app.add(2, 2) == 4

    def test_task_decorator(self, app):
        @app.task
        def add(a, b):
            return a + b

        assert app.add(2, 2) == 4
