# The `WatchbotProgress` low-level interface

The `watchbot_progress.backends.base.WatchbotProgressBase` class defines an abstract interface for all backends to follow.
Its methods implement similar functionality to the equivalent [JavaScript functions in `watchbot_progress`](https://github.com/mapbox/watchbot-progress#reporting-progress-in-javascript)

* `status(jobid)` returns a dictionary with the job status.
* `set_total(jobid, parts)` sets the total number of parts for a reduce job.
* `set_metadata(jobid, meta)` sets arbitrary job metadata from a dictionary
* `fail_job(jobid, reason)` marks the job as failed.
* `complete_part(jobid, partid)` updates the database to mark the part as completed.
* `send_message(jobid, message, subject)` sends an SNS message


The `WatchbotProgressBase` class is not intended to be used directly but as an abstract base class, a template for concrete implementations.

## Writing your own backend

The rules for writing your own backend class are as follows:

* Must inherit from `WatchbotProgressBase`
* Must provide concrete implementations of all the above methods.

Assuming the methods are implmented properly for the backend of choice, you can use it
by passing instances of this class to the `create_job` and `Part` function's `progress` argument.

```python
create_job(parts, progress=MyProgressBackend())
```
