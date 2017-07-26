import pytest
from watchbot_progress import create_job, Part, WatchbotProgress
from mock import patch, PropertyMock

parts = [
    {'source': 'a.tif'},
    {'source': 'b.tif'},
    {'source': 'c.tif'}]


class MockBase(WatchbotProgress):

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

    class MockProgress(MockBase):
        pass

    with patch('watchbot_progress.uuid.uuid4', new=lambda: '000-123'):
        with patch('watchbot_progress.WatchbotProgress', new=MockProgress):
            assert create_job(parts) == '000-123'


def test_Part_job_not_done(monkeypatch):
    monkeypatch.setenv('WorkTopic', 'abc123')
    monkeypatch.setenv('ProgressTable', 'arn::table/foo')

    class MockProgress(MockBase):
        def status(self, x):
            return {'progress': 0.3}

    with patch('watchbot_progress.WatchbotProgress', new=MockProgress):
        msg = {'source': 'a.tif', 'partid': 1, 'jobid': '123'}
        with Part(**msg):
            msg['source']


def test_Part_job_done(monkeypatch):
    monkeypatch.setenv('WorkTopic', 'abc123')
    monkeypatch.setenv('ProgressTable', 'arn::table/foo')

    class MockProgress(MockBase):
        def complete_part(self, jobid, partid):
            return True

    with patch('watchbot_progress.WatchbotProgress', new=MockProgress):
        msg = {'source': 'a.tif', 'partid': 1, 'jobid': '123'}
        with Part(**msg):
            msg['source']


def test_Part_failure(monkeypatch):
    monkeypatch.setenv('WorkTopic', 'abc123')
    monkeypatch.setenv('ProgressTable', 'arn::table/foo')

    msg = {'source': 'a.tif', 'partid': 1, 'jobid': '123'}

    class MockProgress(MockBase):
        def fail_job(self, jobid, reason):
            pass

    class CustomException(Exception):
        pass

    # The exception will get caught by the Part context block and
    # the job will be marked as failed and the exeption re-raised
    with patch('watchbot_progress.WatchbotProgress', new=MockProgress):
        with pytest.raises(CustomException) as e:
            with Part(**msg):
                raise CustomException('processing failed')
        assert 'processing failed' in str(e)
        # TODO assert that fail_job got called


def test_Part_already_failed(monkeypatch):
    monkeypatch.setenv('WorkTopic', 'abc123')
    monkeypatch.setenv('ProgressTable', 'arn::table/foo')

    class MockProgress(MockBase):
        def status(self, x):
            return {'progress': 0.3, 'failed': True}

    with patch('watchbot_progress.WatchbotProgress', new=MockProgress):
        msg = {'source': 'a.tif', 'partid': 1, 'jobid': '123'}
        with pytest.raises(RuntimeError) as e:
            with Part(**msg):
                # sees that the overall job failed and will not execute this code
                raise NotImplementedError("You'll never get here")
        assert 'already failed' in str(e)


def test_create_jobs_metadata(monkeypatch):
    monkeypatch.setenv('WorkTopic', 'abc123')
    monkeypatch.setenv('ProgressTable', 'arn::table/foo')

    class MockProgress(MockBase):
        pass

    meta = {'foo': 'bar'}

    with patch('watchbot_progress.uuid.uuid4', new=lambda: '000-123'):
        with patch('watchbot_progress.WatchbotProgress', new=MockProgress):
            assert create_job(parts, metadata=meta) == '000-123'
