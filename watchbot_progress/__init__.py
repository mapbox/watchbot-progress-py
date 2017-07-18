from __future__ import division

from concurrent import futures
from contextlib import contextmanager
from functools import partial
import json
import logging
import os
import uuid

import boto3


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class WatchbotProgress(object):
    """Sets up objects for reduce mode job tracking with SNS and DynamoDB

    The methods of this object are based on the on the equivalent
    methods in the JavaScript implementation:
    https://github.com/mapbox/watchbot-progress
    """

    def __init__(self, table_arn=None, topic_arn=None):
        # SNS Messages
        self.sns = boto3.client('sns')
        self.topic = topic_arn if topic_arn else os.environ['WorkTopic']

        # DynamoDB
        self.table_arn = table_arn if table_arn else os.environ['ProgressTable']
        if self.table_arn:
            self.table = self.table_arn.split(':table/')[-1]  # just name
        self.dynamodb = boto3.resource('dynamodb')
        self.db = self.dynamodb.Table(self.table)

    def status(self, jobid, part=None):
        """get status from dynamodb

        Parameters
        ----------
        jobid: string?
        part: optional int

        Returns
        -------
        dict, similar to JS watchbot-progress.status object
        """
        res = self.db.get_item(Key={'id': jobid}, ConsistentRead=True)
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

    def set_total(self, jobid, parts):
        """ set total number of parts for the job

        Based on watchbot-progress.setTotal
        """
        total = len(parts)
        return self.db.update_item(
            Key={
                'id': jobid},
            ExpressionAttributeNames={
                '#p': 'parts',
                '#t': 'total'},
            ExpressionAttributeValues={
                ':p': set(range(total)),
                ':t': total},
            UpdateExpression='set #p = :p, #t = :t')

    def fail_job(self, jobid, reason):
        """fail the job, notify dynamodb

        Based on watchbot-progress.failJob
        """
        logger.error('[fail_job] {} failed because {}.'.format(jobid, reason))
        self.db.update_item(
            Key={
                'id': jobid},
            ExpressionAttributeNames={
                '#e': 'error'},
            ExpressionAttributeValues={
                ':e': reason},
            UpdateExpression='set #e = :e')

    def complete_part(self, jobid, partid):
        """Mark part as complete

        Returns
        -------
        boolean
            Is the overall job completed yet?
        """
        res = self.db.update_item(
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

    def set_metadata(self, jobid, metadata):
        """Associate arbitrary metadata with a particular map-reduce job
        """
        raise NotImplementedError()

    def send_message(self, message, subject):
        """Function wrapper to facilitate partial application"""
        return self.sns.publish(
            Message=json.dumps(message),
            Subject=subject,
            TopicArn=self.topic)


#
# The main public interfaces, create_job and Part
#


def create_job(parts, jobid=None, workers=8, table_arn=None, topic_arn=None):
    """Create a reduce mode job

    Handles all the details of reduce-mode accounting (SNS, partid and jobid)
    """

    jobid = jobid if jobid else str(uuid.uuid4())

    progress = WatchbotProgress(table_arn=table_arn, topic_arn=topic_arn)
    progress.set_total(jobid, parts)

    annotated_parts = [
        part.update(partid=partid, jobid=jobid)
        for partid, part in enumerate(parts)]

    # Send SNS message for each part, concurrently
    _send_message = partial(progress.send_message, subject='map')
    with futures.ThreadPoolExecutor(max_workers=workers) as executor:
        executor.map(_send_message, annotated_parts)

    return jobid


@contextmanager
def Part(jobid, partid, table_arn=None, topic_arn=None, **kwargs):
    """Context Manager for parts of an ecs-watchbot reduce job.

    Params
    ------
    jobid
    partid
    table_arn
    topic_arn

    """
    progress = WatchbotProgress(table_arn=table_arn, topic_arn=topic_arn)

    if 'failed' in progress.status(jobid):
        raise RuntimeError('job {} already failed'.format(jobid))

    try:
        # yield control to the context block which processes the message
        yield
    except:
        progress.fail_job(jobid, partid)
        raise
    else:
        all_done = progress.complete_part(jobid, partid)
        if all_done:
            progress.send_message({'jobid': jobid}, subject='reduce')
