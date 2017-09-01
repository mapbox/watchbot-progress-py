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
