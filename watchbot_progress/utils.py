import json

from boto3.session import Session as boto3_session


def chunker(iterable, n):
    """
    Chop list in smaller lists
    """
    for i in range(0, len(iterable), n):
        yield iterable[i:i + n]


def aws_send_message(message, topic, subject=None, client=None):
    """
    Sends SNS message
    """

    if not client:
        session = boto3_session()
        client = session.client('sns')

    return client.publish(
        Message=json.dumps(message),
        Subject=subject,
        TargetArn=topic)


def sns_worker(messages, topic, subject=None):
    """
    Sends batch of SNS messages
    """

    session = boto3_session()
    client = session.client('sns')

    for message in messages:
        aws_send_message(message, topic, subject=subject, client=client)

    return True
