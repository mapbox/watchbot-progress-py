0.9.1
-----
- Pin Redis to 2.x

0.9.0
-----
- Adds ability to provide a custom callback to the Part context manager in place of sending a reduce message

0.8.0
-----
- Modify the Part context manager to ensure the reduce message gets sent only once per job

0.7.0
-----
- Send SNS messages in batch using boto3 thread safe method
- add a utils module
- Implement delete method for redis backend

0.6.0
-----
- Pass kwargs to redis connection
- raise exception for missing jobid

0.5.0
-----
- Refactor WatchbotProgress class to provide pluggable backends (#12)
- Implement a Redis backend (#13)
- Command line interface tools rewritten to use the backend interface.

0.4.0
-----
- BREAKING CHANGE: the `Part` context manager will not fail the entire job by default.
- Added `fail_job_on`, a list exceptions which will mark the job failed.

0.3.0
-----
- `watchbot-progress-py` command line interface for tracking jobs

0.2.1
-----
- send metadata with map and reduce messages #5

0.2.0
-----
- implement `set_metadata` method to associate an arbitrary mapping to a job
- pass `metadata` kwarg to `create_job`
