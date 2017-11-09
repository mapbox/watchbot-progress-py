from __future__ import division
from __future__ import absolute_import

import logging
import os

import redis

from watchbot_progress.backends.base import WatchbotProgressBase
from watchbot_progress.errors import JobDoesNotExist


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class RedisProgress(WatchbotProgressBase):
    """Sets up objects for reduce mode job tracking with SNS and Redis
    """

    def __init__(self, topic_arn=None, host='localhost', port=6379, db=0,
                 delete_when_done=False, **kwargs):
        """Redis-backed progress object

        Parameters
        ----------
        topic_arn
        host: string, redis host
        port: integer
        db: integer, redis db number
        kwargs: passed directly to redis.StrictRedis connection
        """
        # SNS Topic
        self.topic = topic_arn if topic_arn else os.environ['WorkTopic']

        # Redis
        self.redis = redis.StrictRedis(host=host, port=port, db=db, **kwargs)
        self.delete_when_done = delete_when_done

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

        # Pop select keys off the metadata dict, expose at top level
        meta = self._decode_dict(meta)
        failed = meta.pop('failed', None)
        error = meta.pop('error', None)
        try:
            total = int(meta.pop('total'))
        except KeyError:
            raise JobDoesNotExist('Job does not exist, run set_total first')

        percent = (total - remaining) / total
        data = dict(
            metadata=meta.copy(),
            jobid=jobid,
            progress=percent,
            total=total,
            remaining=remaining)

        data['failed'] = (failed == '1')
        if error:
            data['error'] = error

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

    def delete(self, jobid):
        """Delete the reduce job
        """
        # Delete parts and metadata, atomically
        pipe = self.redis.pipeline()
        pipe.delete(self._parts_key(jobid))
        pipe.delete(self._metadata_key(jobid))
        parts_del, meta_del = pipe.execute()
        return (parts_del, meta_del)

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

        if remaining == 0:
            if self.delete_when_done:
                self.delete(jobid)
            return True
        else:
            return False

    def set_metadata(self, jobid, metadata):
        """Associate arbitrary metadata with a particular map-reduce job
        """
        for key, value in metadata.items():
            self.redis.hset(self._metadata_key(jobid), key, value)

    def list_pending_parts(self, jobid):
        """Pending (incomplete) part numbers for a given jobid
        """
        pipe = self.redis.pipeline()
        pipe.hgetall(self._metadata_key(jobid))
        pipe.smembers(self._parts_key(jobid))
        meta, parts = pipe.execute()

        meta = self._decode_dict(meta)
        if 'total' not in meta.keys():
            raise JobDoesNotExist('jobid {} does not exist'.format(jobid))

        return [int(x) for x in parts]

    def list_jobs(self, status=True):
        """Yields all jobs in the database

        If status is True, the yielded items will be the full status dictionary of each job
        If status is False, the items will be job ids only
        """
        postfix = '-metadata'  # see _metadata_key method
        for key in self.redis.scan_iter(match='*' + postfix):
            jobid = key.decode('utf-8').replace(postfix, '')
            if status:
                yield self.status(jobid)
            else:
                yield jobid
