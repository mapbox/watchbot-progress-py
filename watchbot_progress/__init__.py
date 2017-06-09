from __future__ import division

from concurrent import futures
from contextlib import contextmanager
from functools import partial
import os
import uuid

import boto3


# sns_client = boto3.client('sns')

SNSTOPIC = 'arn:::sns-foo'
REGION = 'us-east-1'

TABLE = os.environ.get('ProgressTable', 'pxm-test-Watchbot-progress')
dynamodb = boto3.resource('dynamodb')
db = dynamodb.Table(TABLE)


def status(jobid, part=None):
    '''get status from dynamodb

    Parameters
    ----------
    jobid: string?
    part: optional int

    Returns
    -------
    dict
    Based on the watchbot-progress.status
    '''
    print(f'DB get status of {jobid}')

    res = db.get_item(
        Key={'id': jobid}, ConsistentRead=True)
    item = res['Item']
    remaining = len(item['parts']) if 'parts' in item else 0
    percent = (item['total'] - remaining) / item['total']

    data = {'progress': percent}

    if 'error' in item:
        # failure must have a 'failed' key
        data['failed'] = item['error']
    if 'metadata' in item:
        data['metadata'] = item['metadata']
    if 'reduceSent' in item:
        data['reduceSent'] = item['reduceSent']

    if part is not None:
        # js implementation
        # if (part) response.partComplete =
        # item.parts ? item.parts.values.indexOf(part) === -1 : true;
        raise NotImplementedError()  # todo

    return data


def set_total(jobid, parts):
    ''' set total number of parts for the job

    Based on watchbot-progress.setTotal
    '''
    total = len(parts)
    print(f'DB set total for {jobid} to {total}')
    return db.update_item(
        Key={
            'id': jobid},
        ExpressionAttributeNames={
            '#p': 'parts',
            '#t': 'total'},
        ExpressionAttributeValues={
            ':p': set(range(total)),
            ':t': total},
        UpdateExpression='set #p = :p, #t = :t')


def fail_job(jobid, reason):
    '''fail the job, notify dynamodb

    Based on watchbot-progress.failJob
    '''
    print(f'DB job {jobid} failed because {reason}.')
    db.update_item(
        Key={
            'id': jobid},
        ExpressionAttributeNames={
            '#e': 'error'},
        ExpressionAttributeValues={
            ':e': reason},
        UpdateExpression='set #e = :e')


def complete_part(jobid, partid):
    print(f'DB part {partid} is done')
    res = db.update_item(
        Key={
            'id': jobid},
        ExpressionAttributeNames={
            '#p': 'parts'},
        ExpressionAttributeValues={
            ':p': set([partid])},
        UpdateExpression='delete #p :p',
        ReturnValues='ALL_NEW')

    record = res['Attributes']
    if 'parts' in record and len(record['parts']) > 0:
        complete = False
    else:
        complete = True
    return complete


def set_metadata(jobid, metadata):
    """Associate arbitrary metadata with a particular map-reduce job
    """
    raise NotImplementedError()


def send_message(message, subject, snstopic=SNSTOPIC, region=REGION):
    print(f'SNS Subject {subject}: {message}')


#
# The main public interfaces, create_job and Part
#

def create_job(parts, jobid=None):
    jobid = jobid if jobid else str(uuid.uuid4())

    set_total(jobid, parts)

    annotated_parts = [
        dict(**part, partid=partid, jobid=jobid)
        for partid, part in enumerate(parts)]

    _send_message = partial(send_message, subject='map')

    with futures.ThreadPoolExecutor(max_workers=10) as executor:
        executor.map(_send_message, annotated_parts)

    return jobid


@contextmanager
def Part(message):
    """Context Manager for parts of an ecs-watchbot reduce job

    Params
    ------
    message: dict, must contain a partid and jobid key
    db: dynamodb connection info?
    """
    jobid = message['jobid']
    partid = message['partid']

    if 'failed' in status(jobid):
        raise RuntimeError(f'job {jobid} already failed')

    try:
        # yield control to the context block which processes the message but
        # doesn't need to know anything about the details of reduce mode
        yield
    except:
        fail_job(jobid, partid)
    else:
        all_done = complete_part(jobid, partid)
        if all_done:
            send_message({'jobid': jobid}, 'reduce')
