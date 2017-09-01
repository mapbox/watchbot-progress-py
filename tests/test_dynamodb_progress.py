from mock import patch

import pytest

from watchbot_progress.backends.dynamodb import DynamoProgress as WatchbotProgress
from watchbot_progress.errors import JobDoesNotExist 


parts = [
    {'source': 'a.tif'},
    {'source': 'b.tif'},
    {'source': 'c.tif'}]


@patch('watchbot_progress.backends.dynamodb.boto3.resource')
def test_status(client, monkeypatch):
    monkeypatch.setenv('WorkTopic', 'abc123')
    monkeypatch.setenv('ProgressTable', 'arn::table/foo')

    item = {'Item': {'total': 4, 'error': True}}
    client.return_value.Table.return_value.get_item.return_value = item
    s = WatchbotProgress().status('123')
    assert s['failed']

    item = {'Item': {'total': 4, 'metadata': True}}
    client.return_value.Table.return_value.get_item.return_value = item
    s = WatchbotProgress().status('123')
    assert s['metadata']

    item = {'Item': {'total': 4, 'reduceSent': True}}
    client.return_value.Table.return_value.get_item.return_value = item
    s = WatchbotProgress().status('123')
    assert s['reduceSent']


@patch('watchbot_progress.backends.dynamodb.boto3.resource')
@pytest.mark.parametrize('mock_item, expected', [
    ({'Item': {'parts': [0, 1, 2, 3], 'total': 4}}, 0),
    ({'Item': {'parts': [2, 3], 'total': 4}}, 0.5),
    ({'Item': {'parts': [], 'total': 4}}, 1),
    ({'Item': {'total': 4}}, 1)
])
def test_status_progress(client, mock_item, expected, monkeypatch):
    monkeypatch.setenv('WorkTopic', 'abc123')
    monkeypatch.setenv('ProgressTable', 'arn::table/foo')
    client.return_value.Table.return_value.get_item.return_value = mock_item

    s = WatchbotProgress().status('123')
    assert s['progress'] == expected


@patch('watchbot_progress.backends.dynamodb.boto3.resource')
def test_set_total(client, monkeypatch):
    monkeypatch.setenv('WorkTopic', 'abc123')
    monkeypatch.setenv('ProgressTable', 'arn::table/foo')
    client.return_value.Table.return_value.update_item.return_value = None

    WatchbotProgress().set_total('123', parts)


@patch('watchbot_progress.backends.dynamodb.boto3.resource')
def test_fail_job(client, monkeypatch):
    monkeypatch.setenv('WorkTopic', 'abc123')
    monkeypatch.setenv('ProgressTable', 'arn::table/foo')
    client.return_value.Table.return_value.update_item.return_value = None

    WatchbotProgress().fail_job('123', 'Failed because it is bad')


@patch('watchbot_progress.backends.dynamodb.boto3.client')
def test_send_message(client, monkeypatch):
    monkeypatch.setenv('WorkTopic', 'abc123')
    monkeypatch.setenv('ProgressTable', 'arn::table/foo')
    client.return_value.publish.return_value = None

    WatchbotProgress().send_message('I brought you this message', 'oh hai')


@patch('watchbot_progress.backends.dynamodb.boto3.resource')
def test_complete_part(client, monkeypatch):
    monkeypatch.setenv('WorkTopic', 'abc123')
    monkeypatch.setenv('ProgressTable', 'arn::table/foo')

    item = {'Attributes': {'parts': [], 'total': 4}}
    client.return_value.Table.return_value.update_item.return_value = item
    s = WatchbotProgress().complete_part('123', 1)
    assert s is True


@patch('watchbot_progress.backends.dynamodb.boto3.resource')
def test_incomplete_part(client, monkeypatch):
    monkeypatch.setenv('WorkTopic', 'abc123')
    monkeypatch.setenv('ProgressTable', 'arn::table/foo')

    item = {'Attributes': {'parts': [1, 2], 'total': 4}}
    client.return_value.Table.return_value.update_item.return_value = item
    s = WatchbotProgress().complete_part('123', 1)
    assert s is False


@patch('watchbot_progress.backends.dynamodb.boto3.resource')
def test_set_metadata(client, monkeypatch):
    monkeypatch.setenv('WorkTopic', 'abc123')
    monkeypatch.setenv('ProgressTable', 'arn::table/foo')

    item = 'sentinel'
    client.return_value.Table.return_value.update_item.return_value = item
    WatchbotProgress().set_metadata('123', {'a': 1})
    assert client.called


@patch('watchbot_progress.backends.dynamodb.boto3.resource')
def test_set_list_jobs(client, monkeypatch):
    monkeypatch.setenv('WorkTopic', 'abc123')
    monkeypatch.setenv('ProgressTable', 'arn::table/foo')

    item = {'id': '123', 'total': 4, 'error': True}
    client.return_value.Table.return_value.scan.return_value = {'Items': [item]}
    client.return_value.Table.return_value.get_item.return_value = {'Item': item}

    jobs = list(WatchbotProgress().list_jobs())
    assert len(jobs) == 1
    assert jobs[0]['jobid'] == '123'

    jobs = list(WatchbotProgress().list_jobs(status=False))
    assert len(jobs) == 1
    assert jobs[0] == '123'


@patch('watchbot_progress.backends.dynamodb.boto3.resource')
def test_set_list_pending(client, monkeypatch):
    monkeypatch.setenv('WorkTopic', 'abc123')
    monkeypatch.setenv('ProgressTable', 'arn::table/foo')

    item = {'id': '123', 'parts': [0, 1, 2]}
    client.return_value.Table.return_value.get_item.return_value = {'Item': item}

    parts = list(WatchbotProgress().list_pending_parts('123'))
    assert len(parts) == 3

    client.return_value.Table.return_value.get_item.return_value = {}
    with pytest.raises(JobDoesNotExist):
        parts = list(WatchbotProgress().list_pending_parts('123'))
