from __future__ import division

import abc
import logging

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())

# Python 2 and 3 compat
# see https://stackoverflow.com/a/38668373
ABC = abc.ABCMeta('ABC', (object,), {'__slots__': ()})


class WatchbotProgressBase(ABC):
    """Abstract base class for WatchbotProgress objects
    """

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
    def list_pending_parts(self, jobid):
        """Pending (incomplete) part numbers for a given jobid
        """

    @abc.abstractmethod
    def list_jobs(self, status=True):
        """Lists of all jobs in the database

        If status is True, the returned items will be the full status dictionary of each job
        If status is False, the items will be job ids only
        """
