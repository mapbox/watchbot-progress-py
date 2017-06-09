# watchbot-progress-py

**STATUS: WIP WIP WIP**

`watchbot-progress-py` is a Python module for running [map-reduce jobs on ECS watchbot](https://github.com/mapbox/ecs-watchbot/blob/master/docs/reduce-mode.md)

ECS watchbot's reduce mode allows you to break up a large job into smaller parts, process them individually, and roll them up into a final result. This involves **tracking the state** of the job using a database. Node and command line users can use [watchbot-progress](https://github.com/mapbox/watchbot-progress) for this task. This library brings similar functionality to Python.

## Interface 

* Python versions of the basic function interfaces.
    * `status(jobid, part=None)`
    * `set_total(jobid, parts)`
    * `fail_job(jobid, reason)`
    * `complete_part(jobid, partid)`
    * `set_metadata(jobid, metadata)` (*not implemented yet*)

* Additional high-level features that allow for cleanly separating AWS-interactions from processing code 
    * Functions
        * `send_message(message, subject, snstopic=SNSTOPIC, region=REGION)`
        * `create_job(parts, jobid=None)`
    * Context Managers
        * `with Part(message): ...`

## Examples

Here's an example Python worker script for implementing the three steps of a map reduce job: **start, map and reduce**. The `$Subject` environment variable specifies which step:

```python
import os

# Note the clean distinction between reduce-mode helpers and processing logic
from watchbot_progress import Part, create_job
from mymodule import break_job_into_parts, process_part

subject = os.environ['Subject']
message = os.environ['Message']

if subject == 'start':
    parts = break_job_into_parts(message)
    jobid = create_job(parts)

elif subject == "map":
    with Part(message):
        partid = message['partid']
        jobid = message['partid']
        process_part(message)

elif subject = "reduce":
    jobid = message['partid']
    print(f'DONE with {jobid}')
```
