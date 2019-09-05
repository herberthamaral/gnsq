from __future__ import division, with_statement

import os

import pytest
import gevent
import urllib3

from gnsq import NsqdHTTPClient, Producer
from gnsq.errors import NSQException, NSQNoConnections, NSQInvalid

from integration_server import NsqdIntegrationServer


SLOW_TIMEOUT = int(os.environ.get('SLOW_TIMEOUT', '10'), 10)


@pytest.mark.slow
@pytest.mark.timeout(SLOW_TIMEOUT)
def test_publish():
    with NsqdIntegrationServer() as server:
        producer = Producer(server.tcp_address)
        producer.start()

        for _ in range(100):
            producer.publish('test', b'hi')

        producer.close()
        producer.join()

        conn = NsqdHTTPClient(server.address, server.http_port)
        stats = conn.stats()

        assert stats['topics'][0]['depth'] == 100


@pytest.mark.slow
@pytest.mark.timeout(SLOW_TIMEOUT)
def test_async_publish():
    with NsqdIntegrationServer() as server:
        results = []
        producer = Producer(server.tcp_address)
        producer.start()

        for _ in range(100):
            results.append(producer.publish('test', b'hi', raise_error=False))

        gevent.joinall(results, raise_error=True)
        producer.close()
        producer.join()

        conn = NsqdHTTPClient(server.address, server.http_port)
        stats = conn.stats()

        assert stats['topics'][0]['depth'] == 100


@pytest.mark.slow
@pytest.mark.timeout(SLOW_TIMEOUT)
def test_multipublish():
    with NsqdIntegrationServer() as server:
        producer = Producer(server.tcp_address)
        producer.start()

        for _ in range(10):
            producer.multipublish('test', 10 * [b'hi'])

        producer.close()
        producer.join()

        conn = NsqdHTTPClient(server.address, server.http_port)
        stats = conn.stats()

        assert stats['topics'][0]['depth'] == 100


@pytest.mark.slow
@pytest.mark.timeout(SLOW_TIMEOUT)
def test_async_multipublish():
    with NsqdIntegrationServer() as server:
        results = []
        producer = Producer(server.tcp_address)
        producer.start()

        for _ in range(10):
            result = producer.multipublish(
                'test', 10 * [b'hi'], raise_error=False)
            results.append(result)

        gevent.joinall(results, raise_error=True)
        producer.close()
        producer.join()

        conn = NsqdHTTPClient(server.address, server.http_port)
        stats = conn.stats()

        assert stats['topics'][0]['depth'] == 100


@pytest.mark.slow
@pytest.mark.timeout(SLOW_TIMEOUT)
def test_publish_error():
    with NsqdIntegrationServer() as server:
        producer = Producer(server.tcp_address)
        producer.start()

        with pytest.raises(NSQInvalid):
            producer.publish('test', b'hi', defer=-1000)

        producer.close()
        producer.join()


@pytest.mark.slow
@pytest.mark.timeout(SLOW_TIMEOUT)
def test_tls_publish():
    extra_params = [
        '--tls-required', 'true',
        '--https-address', '127.0.0.1:4152',
    ]
    with NsqdIntegrationServer(extra_params=extra_params) as server:
        producer = Producer(
            server.tcp_address,
            tls_options={
                'keyfile': server.tls_key,
                'certfile': server.tls_cert,
            },
            tls_v1=True,
        )
        producer.start()

        for _ in range(100):
            producer.publish('test', b'hi')

        producer.close()
        producer.join()

        conn = NsqdHTTPClient(
            server.address,
            '4152',
            connection_class=urllib3.HTTPSConnectionPool,
            cert_reqs='CERT_NONE',
        )
        stats = conn.stats()

        assert stats['topics'][0]['depth'] == 100


def test_not_running():
    producer = Producer('192.0.2.1:4150')

    with pytest.raises(NSQException):
        producer.publish('topic', b'hi')


def test_no_connections():
    producer = Producer('192.0.2.1:4150', timeout=0.01)
    producer.start()

    with pytest.raises(NSQNoConnections):
        producer.publish('topic', b'hi', block=False)
