from watchbot_progress import create_job, Part
from watchbot_progress.backends.redis import RedisProgress
from mock import patch
from mockredis import mock_strict_redis_client

parts = [
    {'source': 'a.tif'},
    {'source': 'b.tif'},
    {'source': 'c.tif'}]


@patch('redis.StrictRedis', mock_strict_redis_client)
@patch('watchbot_progress.backends.redis.boto3.client')
def test_progress_one_done(client, monkeypatch):
        monkeypatch.setenv('WorkTopic', 'abc123')
        progress = RedisProgress()

        jobid = create_job(parts, progress=progress)
        with Part(jobid, 0, progress=progress):
            pass

        assert progress.status(jobid)['remaining'] == 2

        # 3 map messages, No reduce message sent
        assert client.return_value.publish.call_count == 3
        sns_args = client.return_value.publish.call_args[1]
        assert sns_args['Subject'] == 'map'


@patch('redis.StrictRedis', mock_strict_redis_client)
@patch('watchbot_progress.backends.redis.boto3.client')
def test_progress_all_done(client, monkeypatch):
        monkeypatch.setenv('WorkTopic', 'abc123')
        progress = RedisProgress()

        jobid = create_job(parts, progress=progress)
        for i in range(3):
            with Part(jobid, i, progress=progress):
                pass

        assert progress.status(jobid)['remaining'] == 0

        # 3 map messages and 1 reduce message sent
        assert client.return_value.publish.call_count == 4
        sns_args = client.return_value.publish.call_args[1]
        assert sns_args['Subject'] == 'reduce'
