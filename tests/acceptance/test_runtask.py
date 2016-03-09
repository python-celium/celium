# -*- coding: utf-8 -*-
import requests
import pytest

from celium import App
from celium import RestServer

app = App()


@app.task
def add(x, y):
    return x + y


@pytest.fixture
def worker():
    app.worker(background=True, node_name='test')
    return app


@pytest.fixture
def rest_server():
    RestServer(background=True)
    return app


class TestTaskAdd(object):

    def test_async(self, rest_server, worker):
        job = app.add.context(queue='my queue', fields={'user': 'foo'}).async(2, 2)

        while not job.finished():
            sleep(0.1)

        assert job.result == 4
        assert requests.get("http://localhost:5222/jobs/{}".format(job.id)).json() == {
            '_id': job.id,
            '_created': "20160801 23:32:32",
            '_status': 'FINISHED',
            '_queue': 'my queue',
            'user': 'foo'
        }
