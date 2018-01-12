from __future__ import division

from concurrent import futures
from contextlib import contextmanager
from functools import partial
import logging
import math
import uuid
import warnings

from watchbot_progress.backends.dynamodb import DynamoProgress
from watchbot_progress.backends.base import WatchbotProgressBase
from watchbot_progress.errors import ProgressTypeError, JobFailed
from watchbot_progress.utils import chunker, sns_worker, aws_send_message


logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

#
# The main public interfaces, create_job and Part
#


def create_job(parts, jobid=None, workers=25, progress=None, metadata=None):
    """Create a reduce mode job

    Handles all the details of reduce-mode accounting (SNS, partid and jobid)

    jobid: string
        Reduce mode job
    partid: string
        Part number
    progress: WatchbotProgress
        Instance of a WatchbotProgress class
        Defaults to DynamoProgress
    """
    if progress is None:
        progress = DynamoProgress()

    if not isinstance(progress, WatchbotProgressBase):
        raise ProgressTypeError(
            'progress must be an instance of WatchbotProgressBase')

    jobid = jobid if jobid else str(uuid.uuid4())

    progress.set_total(jobid, parts)

    if metadata:
        progress.set_metadata(jobid, metadata)

    annotated_parts = []
    for partid, original_part in enumerate(parts):
        part = original_part.copy()
        part.update(partid=partid)
        part.update(jobid=jobid)
        part.update(metadata=metadata)
        annotated_parts.append(part)

    # Create chunks of messages to be processed by each thread
    chunk_size = max(math.ceil(len(annotated_parts) / workers), workers)
    _chunks = chunker(annotated_parts, chunk_size)

    # Send SNS message for each part, concurrently
    _send_message = partial(sns_worker, topic=progress.topic, subject='map')
    with futures.ThreadPoolExecutor(max_workers=workers) as executor:
        executor.map(_send_message, _chunks)

    return jobid


@contextmanager
def Part(jobid, partid, progress=None, fail_job_on=(), on_reduce=None, **kwargs):
    """Context manager to handle parts of an ecs-watchbot reduce job.

    Params
    ------
    jobid: string
        Reduce mode job
    partid: string
        Part number
    progress: WatchbotProgress
        Instance of a WatchbotProgress class
        Defaults to DynamoProgress
    fail_job_on: sequence
        Exception classes which should mark job as failed
    on_reduce: function
        custom callback to run instead of sending
        reduce message to topic
    kwargs: dict
        absorbs additional keywords allowing part dicts
        to be unpacked as input to Part
    """
    if progress is None:
        progress = DynamoProgress()

    if not isinstance(progress, WatchbotProgressBase):
        raise ProgressTypeError(
            'progress must be an instance of WatchbotProgressBase')

    if fail_job_on:
        # Only check for job failure if there are exception types to fail on
        if 'failed' in progress.status(jobid):
            raise JobFailed('job {} already failed'.format(jobid))

    try:
        # yield control to the context block which processes the message
        yield
    except Exception as err:
        if any(isinstance(err, f) for f in fail_job_on):
            progress.fail_job(jobid, partid)
        raise
    else:
        all_done = progress.complete_part(jobid, partid)
        if all_done:
            status = progress.status(jobid)
            metadata = status.get('metadata', {})
            message = {
                'jobid': jobid,
                'metadata': metadata}

            already_sent = metadata.get('reduce_message_sent', False)
            if already_sent:
                warnings.warn('skip reduce message, already sent for job {}'.format(jobid))
            else:
                if on_reduce is None:
                    aws_send_message(message, progress.topic, subject='reduce')
                else:
                    on_reduce(message, progress.topic, subject='reduce')
                progress.set_metadata(jobid, {'reduce_message_sent': True})
