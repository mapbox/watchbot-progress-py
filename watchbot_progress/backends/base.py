from __future__ import division

import abc
import logging

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


class WatchbotProgressBase(object):
    """Abstract base class for WatchbotProgress objects
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def status(self, jobid, part=None):
        """Get status

        Parameters
        ----------
        jobid: string
        part: optional int

        Returns
        -------
        dict, similar to JS watchbot-progress.status object
        """

    @abc.abstractmethod
    def set_total(self, jobid, parts):
        """ set total number of parts for the job
        Based on watchbot-progress.setTotal
        """

    @abc.abstractmethod
    def fail_job(self, jobid, reason):
        """fail the job, notify db
        Based on watchbot-progress.failJob
        """

    @abc.abstractmethod
    def complete_part(self, jobid, partid):
        """Mark part as complete

        Returns
        -------
        boolean
            Is the overall job completed yet?
        """

    @abc.abstractmethod
    def set_metadata(self, jobid, metadata):
        """Associate arbitrary metadata with a particular map-reduce job
        """

    @abc.abstractmethod
    def send_message(self, message, subject):
        """Send SNS message"""
