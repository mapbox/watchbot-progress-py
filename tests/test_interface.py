from watchbot_progress import create_job, Part
from unittest.mock import patch

parts = [
    {'source': 'a.tif'},
    {'source': 'b.tif'},
    {'source': 'c.tif'}]


@patch('watchbot_progress.boto3.client')
@patch('watchbot_progress.boto3.resource')
def test_create_jobs(sns, db, monkeypatch):
    db.update_item.return_value = None
    sns.publish.return_value = None
    monkeypatch.setenv('WorkTopic', 'abc123')
    monkeypatch.setenv('ProgressTable', 'arn::table/foo')
    with patch('watchbot_progress.uuid.uuid4', new=lambda: '000-123'):
        assert create_job(parts) == '000-123'


@patch('watchbot_progress.boto3.client')
@patch('watchbot_progress.boto3.resource')
def test_Part(sns, db, monkeypatch):
    db.update_item.return_value = None
    sns.publish.return_value = None
    monkeypatch.setenv('WorkTopic', 'abc123')
    monkeypatch.setenv('ProgressTable', 'arn::table/foo')

    msg = {'source': 'a.tif', 'partid': 1, 'jobid': '123'}
    with Part(**msg):
        msg['source']
