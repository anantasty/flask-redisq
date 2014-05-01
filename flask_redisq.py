# -*- coding: utf-8 -*-
"""
    flask_redisq
    ~~~~~~~~

    RQ (Redis Queue) integration for Flask applications.

    :copyright: (c) 2012 by Matt Wright.
    :license: MIT, see LICENSE for more details.

"""

__version__ = '0.1'

import redis

from flask import current_app
from redis._compat import urlparse
from rq import Queue, Worker

DEFAULT_CONFIG = {
    'RQ_DEFAULT_HOST': 'localhost',
    'RQ_DEFAULT_PORT': 6379,
    'RQ_DEFAULT_PASSWORD': None,
    'RQ_DEFAULT_DB': 0
}

def _get_config_value(name, key):
    name = name.upper()
    config_key = 'RQ_{}_{}'.format(name, key)
    if not config_key in current_app.config \
          and not 'RQ{}_URL'.format(name) in current_app.config:
      config_key = 'RQ_DEFAULTS_{}'.format(key)
    return current_app.config.get(config_key, None)

def _get_connection(queue='default'):
    url = _get_config_value(queue, 'URL')
    if url:
        return redis.from_url(url, db=_get_config_value(queue, 'DB'))
    return redis.Redis(host=_get_config_value(queue, 'HOST'),
                       port=_get_config_value(queue, 'PORT'),
                       password=_get_config_value(queue, 'PASSWORD'),
                       db=_get_config_value(queue, 'DB'))

def get_queue(name='default', **kwargs):
    kwargs['connection'] = _get_connection(name)
    return Queue(name, **kwargs)

def get_server_url(name):
    url = _get_config_value(name, 'URL')
    if url:
        url_kwargs = urlparse(url)
        return '{}://{}'.format(url_kwargs.scheme, url_kwargs.netloc)
    host = _get_config_value(name, 'HOST')
    password = _get_config_value(name, 'PASSWORD')
    netloc = host if not password else ':{}@{}'.format(password, host)
    return 'redis://{}'.format(netloc)

def get_worker(*queue_names):
    if not queue_names:
        queue_names = ['default']
    return Worker([get_queue(queue_name) for queue_name in queue_names],
                  connection=_get_connection(queue_names[0]))

def job(func_or_queue):
    if callable(func_or_queue):
        func = func_or_queue
        queue = 'default'
    else:
        func = None
        queue = func_or_queue

    def wrapper(fn):
        fn.__module__ = 'app'
        def delay(*args, **kwargs):
            q = get_queue(queue)
            return q.enqueue(fn, *args, **kwargs)

        fn.delay = delay
        return fn

    if func is not None:
        return wrapper(func)

    return wrapper


class RQ(object):
    def __init__(self, app=None):
        if app is not None:
            self.init_app(app)

    def init_app(self, app):
        for key, value in DEFAULT_CONFIG.items():
            app.config.setdefault(key, value)
