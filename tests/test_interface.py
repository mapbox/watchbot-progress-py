import pytest
from watchbot_progress import create_job, Part
from watchbot_progress.errors import JobFailed, ProgressTypeError
from watchbot_progress.backends.base import WatchbotProgressBase
from mock import patch

parts = [
    {'source': 'a.tif'},
    {'source': 'b.tif'},
    {'source': 'c.tif'}]


class MockProgress(WatchbotProgressBase):

    def __init__(self, topic_arn=None):
        self.topic = topic_arn

    def status(self, x):
        return {'progress': 0.3}

    def set_total(self, jobid, parts):
        pass

    def fail_job(self, jobid, reason):
        pass

    def complete_part(self, jobid, partid):
        return False

    def set_metadata(self, jobid, metdata):
        return None

    def list_jobs(self, jobid, metdata):
        return []

    def list_pending_parts(self, jobid, metdata):
        return []


@pytest.fixture
def CustomCallback():
    class OnReduce():
        def __init__(self):
            self.called = False
            pass
        def on_reduce(self, message, topic, subject):
            self.called = True
            self.message = message
            self.topic = topic
            self.subject = subject
        def assert_called_once(self):
            assert self.called

    return OnReduce()


@patch('watchbot_progress.main.sns_worker')
def test_create_jobs(sns_worker, monkeypatch):
    monkeypatch.setenv('WorkTopic', 'abc123')
    monkeypatch.setenv('ProgressTable', 'arn::table/foo')

    sns_worker.side_effect = [True]

    with patch('uuid.uuid4', new=lambda: '000-123'):
        assert create_job(parts, progress=MockProgress()) == '000-123'
    sns_worker.assert_called_once()
    assert len(sns_worker.call_args[0][0]) == 3
    assert sns_worker.call_args[1].get('subject') == 'map'


@patch('watchbot_progress.main.aws_send_message')
def test_Part_job_not_done(aws_send_message, monkeypatch):
    monkeypatch.setenv('WorkTopic', 'abc123')
    monkeypatch.setenv('ProgressTable', 'arn::table/foo')

    with Part(jobid='123', partid=1, progress=MockProgress()):
        pass
    aws_send_message.assert_not_called()


@patch('watchbot_progress.main.aws_send_message')
def test_Part_job_done(aws_send_message, monkeypatch):
    monkeypatch.setenv('WorkTopic', 'abc123')
    monkeypatch.setenv('ProgressTable', 'arn::table/foo')

    class CustomProgress(MockProgress):
        def complete_part(self, jobid, partid):
            return True

    with Part(jobid='123', partid=1, progress=CustomProgress()):
        pass
    aws_send_message.assert_called_once()
    assert aws_send_message.call_args[1].get('subject') == 'reduce'


def test_Part_job_done_on_reduce_callback(CustomCallback, monkeypatch):
    monkeypatch.setenv('WorkTopic', 'abc123')
    monkeypatch.setenv('ProgressTable', 'arn::table/foo')

    class CustomProgress(MockProgress):
        def complete_part(self, jobid, partid):
            return True

    with Part(jobid='123', partid=1, progress=CustomProgress(), on_reduce=CustomCallback.on_reduce):
        pass

    CustomCallback.assert_called_once()
    assert CustomCallback.message == {'jobid': '123', 'metadata':{}}
    assert CustomCallback.topic is None
    assert CustomCallback.subject == 'reduce'


@patch('watchbot_progress.main.aws_send_message')
def test_Part_already_failed(aws_send_message, monkeypatch):
    monkeypatch.setenv('WorkTopic', 'abc123')
    monkeypatch.setenv('ProgressTable', 'arn::table/foo')

    class CustomProgress(MockProgress):
        def status(self, x):
            return {'progress': 0.3, 'failed': True}

    class CustomException(Exception):
        pass

    msg = {'source': 'a.tif', 'partid': 1, 'jobid': '123'}
    with pytest.raises(JobFailed) as e:
        with Part(fail_job_on=[CustomException], progress=CustomProgress(), **msg):
            # sees that the overall job failed and will not execute this code
            raise NotImplementedError("You'll never get here")
    assert 'already failed' in str(e)
    aws_send_message.assert_not_called()


@patch('watchbot_progress.main.sns_worker')
def test_create_jobs_metadata(sns_worker, monkeypatch):
    monkeypatch.setenv('WorkTopic', 'abc123')
    monkeypatch.setenv('ProgressTable', 'arn::table/foo')

    meta = {'foo': 'bar'}

    with patch('uuid.uuid4', new=lambda: '000-123'):
        assert create_job(parts, progress=MockProgress(), metadata=meta) == '000-123'
    sns_worker.assert_called_once()
    assert len(sns_worker.call_args[0][0]) == 3
    assert sns_worker.call_args[1].get('subject') == 'map'


@patch('watchbot_progress.main.aws_send_message')
@patch('watchbot_progress.main.DynamoProgress.fail_job')
@patch('watchbot_progress.main.DynamoProgress.status')
def test_Part_fail_job_on(status, fail_job, aws_send_message, monkeypatch):
    monkeypatch.setenv('WorkTopic', 'abc123')
    monkeypatch.setenv('ProgressTable', 'arn::table/foo')

    class CustomException(Exception):
        pass

    # the job WILL be marked as failed, the exeption is still re-raised
    with pytest.raises(CustomException):
        with Part(jobid=2, partid=2, fail_job_on=[CustomException]):
            raise CustomException()
    fail_job.assert_called_once_with(2, 2)
    aws_send_message.assert_not_called()


@patch('watchbot_progress.main.aws_send_message')
@patch('watchbot_progress.main.DynamoProgress.fail_job')
@patch('watchbot_progress.main.DynamoProgress.status')
def test_Part_dont_fail_job_on(status, fail_job, aws_send_message, monkeypatch):
    monkeypatch.setenv('WorkTopic', 'abc123')
    monkeypatch.setenv('ProgressTable', 'arn::table/foo')

    class CustomException(Exception):
        pass

    # the job will NOT be marked as failed, the exeption is still re-raised
    with pytest.raises(CustomException):
        with Part(jobid=2, partid=2, fail_job_on=[ZeroDivisionError]):
            raise CustomException()
    fail_job.assert_not_called()
    aws_send_message.assert_not_called()


@patch('watchbot_progress.main.aws_send_message')
def test_Part_bad_progress(aws_send_message):
    with pytest.raises(ProgressTypeError):
        with Part(partid=1, jobid='1', progress=Exception()):
            pass
    aws_send_message.assert_not_called()


@patch('watchbot_progress.main.sns_worker')
def test_create_bad_progress(sns_worker):
    with pytest.raises(ProgressTypeError):
        create_job(jobid='1', parts=parts, progress=Exception())
    sns_worker.assert_not_called()
