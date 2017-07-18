# watchbot-progress-py

`watchbot-progress-py` is a Python module for running [map-reduce jobs on ECS watchbot](https://github.com/mapbox/ecs-watchbot/blob/master/docs/reduce-mode.md)

ECS watchbot's reduce mode allows you to break up a large job into smaller parts, process them individually, and roll them up into a final result. This involves tracking the state of the job using a database. Node and command line users can use [watchbot-progress](https://github.com/mapbox/watchbot-progress) for this task. This library brings similar functionality to Python users and provides a high-level interface which cleanly separates reduce-mode logic from your processing code.

### Low-level Interface 

The low-level interface is provided by the `WatchbotProgress` class.

```python
progress =  WatchbotProgress(table_arn=..., topic_arn=...)
```

If the AWS ARNs are not specified, the SNS topic and the DynamoDB table will be determined from the `WorkTopic` and `ProgressTable` environment variables.

An instance of `WatchbotProgress` has methods which implement the same functionality as the JavaScript functions in `watchbot_progress`:

* `progress.status(jobid)` returns a dictionary with the job status.
* `progress.set_total(jobid, parts)` sets the total number of parts for a reduce job.
* `progress.fail_job(jobid, reason)` marks the job as failed.
* `progress.complete_part(jobid, partid)` updates the database to mark the part as completed.

### High-level interface (recommended)

The high-level interface allows you to cleanly separate AWS-interactions from processing code.

* The `create_job` function breaks a list of parts into separate jobs and handles the details of reduce-mode accounting.

* The `Part` context manager wraps the processing of each part, and handles all cases such as failures, success and completion of the overall job.

The only restriction is that each **part** must be a mutable mapping (e.g. a dictionary) which is json serializable. The watchbot_progress functions will append the `jobid` and `partid` keys to the dictionary for accounting purposes and are not allowed. All other keys are yours to choose, provided their values can be used in `json.dumps`.

## Example

Here's an example Python worker script for implementing the three steps of a map reduce job: **start, map and reduce**. The `$Subject` environment variable specifies which step:

```python
import os

# Note the distinction between reduce-mode helpers..
from watchbot_progress import Part, create_job
# ... and processing logic
from mymodule import break_job_into_parts, process_part

subject = os.environ['Subject']
message = json.loads(os.environ['Message'])

if subject == 'start':
    # Each part is a dictionary with keys of your chosing
    parts = break_job_into_parts(message)
    jobid = create_job(parts)

elif subject == "map":
    jobid = message['jobid']
    partid = message['partid']
    with Part(jobid, partid):
        process_part(message)

elif subject = "reduce":
    jobid = message['jobid']
    print(f'DONE with {jobid}')
```
