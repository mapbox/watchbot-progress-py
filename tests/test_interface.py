import pytest
from watchbot_progress import create_job, Part
from watchbot_progress.main import JobFailed, ProgressTypeError
from watchbot_progress.backends.base import WatchbotProgressBase
from mock import patch

parts = [
    {'source': 'a.tif'},
    {'source': 'b.tif'},
    {'source': 'c.tif'}]


class MockProgress(WatchbotProgressBase):

    def status(self, x):
        return {'progress': 0.3}

    def set_total(self, jobid, parts):
        pass

    def fail_job(self, jobid, reason):
        pass

    def complete_part(self, jobid, partid):
        return False

    def send_message(self, message, subject):
        return True

    def set_metadata(self, jobid, metdata):
        return None


def test_create_jobs(monkeypatch):
    monkeypatch.setenv('WorkTopic', 'abc123')
    monkeypatch.setenv('ProgressTable', 'arn::table/foo')

    with patch('uuid.uuid4', new=lambda: '000-123'):
        assert create_job(parts, progress=MockProgress()) == '000-123'


def test_Part_job_not_done(monkeypatch):
    monkeypatch.setenv('WorkTopic', 'abc123')
    monkeypatch.setenv('ProgressTable', 'arn::table/foo')

    with Part(jobid='123', partid=1, progress=MockProgress()):
        pass


def test_Part_job_done(monkeypatch):
    monkeypatch.setenv('WorkTopic', 'abc123')
    monkeypatch.setenv('ProgressTable', 'arn::table/foo')

    class CustomProgress(MockProgress):
        def complete_part(self, jobid, partid):
            return True

    with Part(jobid='123', partid=1, progress=CustomProgress()):
        pass


def test_Part_already_failed(monkeypatch):
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


def test_create_jobs_metadata(monkeypatch):
    monkeypatch.setenv('WorkTopic', 'abc123')
    monkeypatch.setenv('ProgressTable', 'arn::table/foo')

    meta = {'foo': 'bar'}

    with patch('uuid.uuid4', new=lambda: '000-123'):
        assert create_job(parts, progress=MockProgress(), metadata=meta) == '000-123'


@patch('watchbot_progress.main.DynamoProgress.fail_job')
@patch('watchbot_progress.main.DynamoProgress.status')
def test_Part_fail_job_on(status, fail_job, monkeypatch):
    monkeypatch.setenv('WorkTopic', 'abc123')
    monkeypatch.setenv('ProgressTable', 'arn::table/foo')

    class CustomException(Exception):
        pass

    # the job WILL be marked as failed, the exeption is still re-raised
    with pytest.raises(CustomException):
        with Part(jobid=2, partid=2, fail_job_on=[CustomException]):
            raise CustomException()
    fail_job.assert_called_once_with(2, 2)


@patch('watchbot_progress.main.DynamoProgress.fail_job')
@patch('watchbot_progress.main.DynamoProgress.status')
def test_Part_dont_fail_job_on(status, fail_job, monkeypatch):
    monkeypatch.setenv('WorkTopic', 'abc123')
    monkeypatch.setenv('ProgressTable', 'arn::table/foo')

    class CustomException(Exception):
        pass

    # the job will NOT be marked as failed, the exeption is still re-raised
    with pytest.raises(CustomException):
        with Part(jobid=2, partid=2, fail_job_on=[ZeroDivisionError]):
            raise CustomException()
    fail_job.assert_not_called()


def test_Part_bad_progress():
    with pytest.raises(ProgressTypeError):
        with Part(partid=1, jobid='1', progress=Exception()):
            pass


def test_create_bad_progress():
    with pytest.raises(ProgressTypeError):
        create_job(jobid='1', parts=parts, progress=Exception())
