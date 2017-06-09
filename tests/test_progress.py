import pytest
from watchbot_progress import create_job, Part
from watchbot_progress import status, set_metadata
from mock import patch

parts = [
    {'source': 'a.tif'},
    {'source': 'b.tif'},
    {'source': 'c.tif'}]


@patch('watchbot_progress.db')
def test_status_extra(client):
    item = {'Item': {'total': 4, 'error': True}}
    client.get_item.return_value = item
    s = status('123')
    assert s['failed']

    item = {'Item': {'total': 4, 'metadata': True}}
    client.get_item.return_value = item
    s = status('123')
    assert s['metadata']

    item = {'Item': {'total': 4, 'reduceSent': True}}
    client.get_item.return_value = item
    s = status('123')
    assert s['reduceSent']


@patch('watchbot_progress.db')
@pytest.mark.parametrize('mock_item, expected', [
    ({'Item': {'parts': [0, 1, 2, 3], 'total': 4}}, 0),
    ({'Item': {'parts': [2, 3], 'total': 4}}, 0.5),
    ({'Item': {'parts': [], 'total': 4}}, 1),
    ({'Item': {'total': 4}}, 1)
])
def test_status_progress(client, mock_item, expected):
    client.get_item.return_value = mock_item
    s = status('123')
    assert s['progress'] == expected


@patch('watchbot_progress.db')
def test_create_jobs(client):
    client.update_item.return_value = None
    with patch('watchbot_progress.uuid.uuid4', new=lambda: '000-123'):
        assert create_job(parts) == '000-123'


@patch('watchbot_progress.db')
def test_part_context_done(client):
    # The status call
    item = {'Item': {'parts': [2, 3], 'total': 4}}
    client.get_item.return_value = item

    # The complete_part call
    client.update_item.return_value = \
        {'Attributes': {'parts': []}}

    msg = {'source': 'a.tif', 'partid': 1, 'jobid': '123'}
    with Part(msg):
        msg['source']


@patch('watchbot_progress.db')
def test_part_context_notdone(client):
    item = {'Item': {'parts': [2, 3], 'total': 4}}
    client.get_item.return_value = item
    client.update_item.return_value = \
        {'Attributes': {'parts': [0, 1, 2]}}

    msg = {'source': 'a.tif', 'partid': 1, 'jobid': '123'}
    with Part(msg):
        msg['source']


@patch('watchbot_progress.db')
def test_part_exception(client):
    item = {'Item': {'parts': [2, 3], 'total': 4}}
    client.get_item.return_value = item

    client.update_item.return_value = \
        {'Attributes': {'parts': [0, 1, 2]}}

    msg = {'source': 'a.tif', 'partid': 1, 'jobid': '123'}
    with Part(msg):
        raise Exception()


@pytest.mark.parametrize('msg', [
    {'source': 'a.tif', 'partid': '1'},
    {'source': 'a.tif', 'jobid': '1'},
])
def test_part_bad(msg):
    with pytest.raises(KeyError):
        with Part(msg):
            pass


@patch('watchbot_progress.db')
def test_part_jobfailed(client):
    item = {'Item': {'total': 4, 'error': True}}
    client.get_item.return_value = item

    msg = {'source': 'a.tif', 'partid': 1, 'jobid': '1'}
    with pytest.raises(RuntimeError):
        with Part(msg):
            pass


@patch('watchbot_progress.db')
@pytest.mark.xfail(raises=NotImplementedError)
def test_status_part(client):
    mock_item = {'Item': {'parts': [], 'total': 4}}
    client.get_item.return_value = mock_item
    status('123', 1)


@pytest.mark.xfail(raises=NotImplementedError)
def test_set_metadata():
    set_metadata('123', {'a': 1})
