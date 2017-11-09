from __future__ import division

import logging
import os

import boto3

from watchbot_progress.backends.base import WatchbotProgressBase
from watchbot_progress.errors import JobDoesNotExist

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class DynamoProgress(WatchbotProgressBase):
    """Sets up objects for reduce mode job tracking with SNS and DynamoDB

    The methods of this object are based on the on the equivalent
    methods in the JavaScript implementation:
    https://github.com/mapbox/watchbot-progress
    """

    def __init__(self, table_arn=None, topic_arn=None):
        # SNS Topic
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
        jobid: string
        part: optional int

        Returns
        -------
        dict, similar to JS watchbot-progress.status object
        """
        res = self.db.get_item(Key={'id': jobid}, ConsistentRead=True)
        item = res['Item']
        remaining = len(item['parts']) if 'parts' in item else 0
        percent = (item['total'] - remaining) / item['total']

        data = {
            'jobid': jobid,
            'progress': percent}

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
            Key={'id': jobid},
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
            Key={'id': jobid},
            ExpressionAttributeNames={'#e': 'error'},
            ExpressionAttributeValues={':e': reason},
            UpdateExpression='set #e = :e')

    def complete_part(self, jobid, partid):
        """Mark part as complete

        Returns
        -------
        boolean
            Is the overall job completed yet?
        """
        res = self.db.update_item(
            Key={'id': jobid},
            ExpressionAttributeNames={'#p': 'parts'},
            ExpressionAttributeValues={':p': set([partid])},
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
        self.db.update_item(
            Key={'id': jobid},
            ExpressionAttributeNames={'#m': 'metadata'},
            ExpressionAttributeValues={':m': metadata},
            UpdateExpression='set #m = :m')

    def delete(self, jobid):
        """Delete the reduce job
        """
        raise NotImplementedError("delete not implemented for dynamodb yet")

    def list_pending_parts(self, jobid):
        """Pending (incomplete) part numbers for a given jobid
        """
        res = self.db.get_item(Key={'id': jobid}, ConsistentRead=True)
        if 'Error' in res or 'Item' not in res:
            raise JobDoesNotExist('jobid {} does not exist')
        pending = res['Item'].get('parts', [])
        return [int(p) for p in pending]

    def list_jobs(self, status=True):
        """Lists of all jobs in the database

        If status is True, the returned items will be the full status dictionary of each job
        If status is False, the items will be job ids only
        """
        scan = self.db.scan(ConsistentRead=True)
        for s in scan['Items']:
            if status:
                yield self.status(s['id'])
            else:
                yield s['id']
