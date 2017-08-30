from __future__ import division

from mock import patch

from mockredis import mock_strict_redis_client
import pytest

from watchbot_progress.backends.redis import RedisProgress
from watchbot_progress.errors import JobDoesNotExist


@pytest.fixture()
def parts():
    return [
        {'source': 'a.tif'},
        {'source': 'b.tif'},
        {'source': 'c.tif'}]


def test_status_no_total(monkeypatch):
    """ Have not created a job with .set_total() yet
    """
    monkeypatch.setenv('WorkTopic', 'abc123')
    with patch('redis.StrictRedis', mock_strict_redis_client):
        with pytest.raises(JobDoesNotExist):
            RedisProgress().status('123')


def test_status(parts, monkeypatch):
    """New job shows all parts remaining
    """
    monkeypatch.setenv('WorkTopic', 'abc123')
    with patch('redis.StrictRedis', mock_strict_redis_client):
        p = RedisProgress()
        p.set_total('123', parts)
        status = p.status('123')
        assert status['total'] == 3
        assert status['remaining'] == 3
        assert status['progress'] == 0


def test_status_part(parts, monkeypatch):
    """Check if part is complete
    """
    monkeypatch.setenv('WorkTopic', 'abc123')
    with patch('redis.StrictRedis', mock_strict_redis_client):
        p = RedisProgress()
        p.set_total('123', parts)
        p.complete_part('123', 0)

        assert p.status('123', part=0)['complete'] is True
        assert p.status('123', part=1)['complete'] is False


def test_status_complete_some(parts, monkeypatch):
    """job shows partial progress
    """
    monkeypatch.setenv('WorkTopic', 'abc123')
    with patch('redis.StrictRedis', mock_strict_redis_client):
        p = RedisProgress()
        p.set_total('123', parts)
        done_yet = p.complete_part('123', 0)
        assert not done_yet

        status = p.status('123')
        assert status['total'] == 3
        assert status['remaining'] == 2
        assert status['progress'] == 1 / 3


def test_status_complete_all(parts, monkeypatch):
    """job shows complete progress
    """
    monkeypatch.setenv('WorkTopic', 'abc123')
    with patch('redis.StrictRedis', mock_strict_redis_client):
        p = RedisProgress()
        p.set_total('123', parts)
        for i, _ in enumerate(parts):
            done_yet = p.complete_part('123', i)
        assert done_yet

        status = p.status('123')
        assert status['total'] == 3
        assert status['remaining'] == 0
        assert status['progress'] == 1.0


def test_failjob(parts, monkeypatch):
    """Mark job as failed works
    """
    monkeypatch.setenv('WorkTopic', 'abc123')
    with patch('redis.StrictRedis', mock_strict_redis_client):
        p = RedisProgress()
        p.set_total('123', parts)
        p.fail_job('123', 'epic fail')
        assert p.status('123')['failed'] is True


def test_metadata(parts, monkeypatch):
    """Setting and getting job metadata works
    """
    monkeypatch.setenv('WorkTopic', 'abc123')
    with patch('redis.StrictRedis', mock_strict_redis_client):
        p = RedisProgress()
        p.set_total('123', parts)
        p.set_metadata('123', {'test': 'foo'})
        assert p.status('123')['test'] == 'foo'


@patch('watchbot_progress.backends.redis.boto3.client')
def test_send_message(client, monkeypatch):
    monkeypatch.setenv('WorkTopic', 'abc123')
    client.return_value.publish.return_value = None
    RedisProgress().send_message('I brought you this message', 'oh hai')
    client.assert_called_once_with('sns')
