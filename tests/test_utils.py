
# import pytest

from mock import patch

from watchbot_progress import utils


def test_chunker_valid():
    """ Should return a valid list of list
    """
    it = [1, 2, 3, 4, 5, 6, 7]
    assert list(utils.chunker(it, 2)) == [[1, 2], [3, 4], [5, 6], [7]]


@patch('watchbot_progress.utils.boto3_session')
def test_aws_send_message_valid(session):
    """ Should work as expected
    """

    session.return_value.client.return_value.publish.return_value = {
        "MessageId": "000000000000000"}

    message = {'content': 'this is a message'}
    topic = "arn:aws:sns:my-region:00000000000:a-stack-0000XXXXXXX"

    assert utils.aws_send_message(message, topic)
    session.assert_called_once()


@patch('watchbot_progress.utils.boto3_session.client')
def test_aws_send_message_valid_client(client):
    """ Should work as expected with client option
    """

    client.return_value.publish.return_value = {
        "MessageId": "000000000000000"}

    message = {'content': 'this is a message'}
    topic = "arn:aws:sns:my-region:00000000000:a-stack-0000XXXXXXX"

    assert utils.aws_send_message(message, topic, client=client())
    client.assert_called_once()


@patch('watchbot_progress.utils.boto3_session')
def test_aws_send_message_valid_subject(session):
    """ Should work as expected with subject option
    """

    session.return_value.return_value.publish.return_value = {
        "MessageId": "000000000000000"}

    message = {'content': 'this is a message'}
    subject = 'map'
    topic = "arn:aws:sns:my-region:00000000000:a-stack-0000XXXXXXX"

    assert utils.aws_send_message(message, topic, subject)
    session.assert_called_once()


@patch('watchbot_progress.utils.aws_send_message')
def test_sns_worker_valid(aws_send_message):
    """ Should work as expected
    """

    aws_send_message.side_effect = [True, True]

    messages = [
        {'content': 'this is a message'},
        {'content': 'this is another message'}]

    topic = "arn:aws:sns:my-region:00000000000:a-stack-0000XXXXXXX"

    assert utils.sns_worker(messages, topic)
    assert aws_send_message.call_count == 2


@patch('watchbot_progress.utils.aws_send_message')
def test_sns_worker_valid_subject(aws_send_message):
    """ Should work as expected
    """

    aws_send_message.side_effect = [True, True]

    messages = [
        {'content': 'this is a message'},
        {'content': 'this is another message'}]
    subject = 'map'
    topic = "arn:aws:sns:my-region:00000000000:a-stack-0000XXXXXXX"

    assert utils.sns_worker(messages, topic, subject)
    assert aws_send_message.call_count == 2
