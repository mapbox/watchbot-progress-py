from watchbot_progress import create_job, Part
from watchbot_progress.backends.redis import RedisProgress
from mock import patch
from mockredis import mock_strict_redis_client

parts = [
    {'source': 'a.tif'},
    {'source': 'b.tif'},
    {'source': 'c.tif'}]


@patch('redis.StrictRedis', mock_strict_redis_client)
@patch('watchbot_progress.main.sns_worker')
@patch('watchbot_progress.main.aws_send_message')
def test_progress_one_done(aws_send_message, sns_worker, monkeypatch):
        monkeypatch.setenv('WorkTopic', 'abc123')

        sns_worker.side_effect = [True]

        progress = RedisProgress()

        jobid = create_job(parts, progress=progress)
        with Part(jobid, 0, progress=progress):
            pass

        assert progress.status(jobid)['remaining'] == 2

        # 3 map messages, No reduce message sent
        sns_worker.assert_called_once()
        assert len(sns_worker.call_args[0][0]) == 3
        assert sns_worker.call_args[1].get('subject') == 'map'
        aws_send_message.assert_not_called()


@patch('redis.StrictRedis', mock_strict_redis_client)
@patch('watchbot_progress.main.sns_worker')
@patch('watchbot_progress.main.aws_send_message')
def test_progress_all_done(aws_send_message, sns_worker, monkeypatch):
        monkeypatch.setenv('WorkTopic', 'abc123')
        progress = RedisProgress()

        jobid = create_job(parts, progress=progress)
        for i in range(3):
            with Part(jobid, i, progress=progress):
                pass

        assert progress.status(jobid)['remaining'] == 0

        # 3 map messages and 1 reduce message sent
        sns_worker.assert_called_once()
        assert len(sns_worker.call_args[0][0]) == 3
        assert sns_worker.call_args[1].get('subject') == 'map'
        aws_send_message.assert_called_once()
        assert aws_send_message.call_args[1].get('subject') == 'reduce'
