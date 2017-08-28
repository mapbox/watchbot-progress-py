from __future__ import division

import json
import logging
import os

import boto3
import redis

from watchbot_progress.backends.base import WatchbotProgressBase
from watchbot_progress.main import JobDoesNotExist


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class RedisProgress(WatchbotProgressBase):
    """Sets up objects for reduce mode job tracking with SNS and Redis
    """

    def __init__(self, topic_arn=None, host='localhost', port=6379, db=0):
        # SNS Messages
        self.sns = boto3.client('sns')
        self.topic = topic_arn if topic_arn else os.environ['WorkTopic']

        # Redis
        self.redis = redis.StrictRedis(host=host, port=port, db=db)

    def _metadata_key(self, jobid):
        return '{}-metadata'.format(jobid)

    def _parts_key(self, jobid):
        return '{}-parts'.format(jobid)

    def _decode_dict(self, meta):
        return {k.decode('utf-8'): v.decode('utf-8')
                for k, v in meta.items()}

    def status(self, jobid, part=None):
        """get status from dynamodb

        Parameters
        ----------
        jobid: string?
        part: optional int
            return status of the given partid

        Returns
        -------
        dict, similar to JS watchbot-progress.status object
        """
        if part is not None:
            is_member = self.redis.sismember(self._parts_key(jobid), part)
            return {
                'part': part,
                'complete': not bool(is_member)}

        pipe = self.redis.pipeline()
        pipe.hgetall(self._metadata_key(jobid))
        pipe.scard(self._parts_key(jobid))
        meta, remaining = pipe.execute()

        meta = self._decode_dict(meta)
        try:
            total = int(meta['total'])
        except KeyError:
            raise JobDoesNotExist('Job does not exist, run set_total first')

        percent = (total - remaining) / total
        data = meta.copy()
        data.update(
            progress=percent,
            total=total,
            remaining=remaining)

        if 'failed' in data:
            data['failed'] = (data['failed'] == '1')

        return data

    def set_total(self, jobid, parts):
        """Set up parts for the job

        Based on watchbot-progress.setTotal
        """
        total = len(parts)
        partids = range(total)

        pipe = self.redis.pipeline()
        pipe.hset(self._metadata_key(jobid), 'total', total)
        pipe.sadd(self._parts_key(jobid), *partids)
        pipe.execute()

    def fail_job(self, jobid, reason):
        """fail the job, notify dynamodb

        Based on watchbot-progress.failJob
        """
        logger.error('[fail_job] {} failed because {}.'.format(jobid, reason))
        self.redis.hset(self._metadata_key(jobid), 'error', reason)
        self.redis.hset(self._metadata_key(jobid), 'failed', 1)

    def complete_part(self, jobid, partid):
        """Mark part as complete

        Returns
        -------
        boolean
            Is the overall job completed yet?
        """
        # Delete and count, atomically
        pipe = self.redis.pipeline()
        pipe.srem(self._parts_key(jobid), partid)
        pipe.scard(self._parts_key(jobid))
        _, remaining = pipe.execute()

        return remaining == 0

    def set_metadata(self, jobid, metadata):
        """Associate arbitrary metadata with a particular map-reduce job
        """
        for key, value in metadata.items():
            self.redis.hset(self._metadata_key(jobid), key, value)

    def send_message(self, message, subject):
        """Function wrapper to facilitate partial application"""
        return self.sns.publish(
            Message=json.dumps(message),
            Subject=subject,
            TopicArn=self.topic)
