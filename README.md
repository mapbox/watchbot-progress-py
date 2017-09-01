# watchbot-progress-py

[![Build Status](https://travis-ci.org/mapbox/watchbot-progress-py.svg?branch=master)](https://travis-ci.org/mapbox/watchbot-progress-py)
[![Coverage Status](https://coveralls.io/repos/github/mapbox/watchbot-progress-py/badge.svg)](https://coveralls.io/github/mapbox/watchbot-progress-py)
[![PyPI version](https://badge.fury.io/py/watchbot-progress.svg)](https://badge.fury.io/py/watchbot-progress)

`watchbot-progress-py` is a Python module for tracking progress of [map-reduce jobs on ECS watchbot](https://github.com/mapbox/ecs-watchbot/blob/master/docs/reduce-mode.md)

ECS watchbot's reduce mode allows you to break up a large job into smaller parts, process them individually, and roll them up into a final result. This involves tracking the state of the job using a database. Node and command line users can use [watchbot-progress](https://github.com/mapbox/watchbot-progress) for this task. This library brings similar functionality to Python users and provides a high-level interface which cleanly separates reduce mode logic from your processing code.

## Usage

### 1. Make a list of parts

Each *Part* is a dictionary containing information about a portion of the job. For example, a part could represent the URL to a file to be processed. The part dict must be JSON-serializable.

```python
parts = [
    {'url': 'https://a.com'},
    {'url': 'https://b.com'},
    {'url': 'https://c.com'}]
```

### 2. Start the job


The `create_job` function takes a list of parts and starts off the process:

* Adds a partid and jobid to each part dictionary.
* Writes the parts and metadata to the backend database which tracks the progress of the overall job.
* Sends an SNS message with a `Subject=map` and with a Message: the json representation of the part dictionary including partid and jobid.
* The jobid is returned.


```python
from watchbot_progress import create_job

jobid = create_job(parts)
```

### 3. Process each part

In your distributed processing code, the code which *receives* the `Subject=map` SNS message,
you can use the `Part` context manager to handle the reduce mode accounting.

* In the case that the context block succeeds:
    * the part will be marked as complete in the backend database
    * the total remaining parts is queried
    * *if* there are no remaining parts, an SNS message with `Subject=reduce` is sent.

* If the context block fails:
    * By default, the failure will not affect the backend database or job status; all remaining parts will be processed and external retry logic applies.
    * If you pass the optional `fail_job_on` parameter, you can specify a list of Exception types which *will* cause the job to fail. All subqeuent parts will be skipped in the event of a job failure.
    * The original exception is re-raised from the context manager

```python
from watchbot_progress import Part

message = json.loads(os.environ['Message'])
jobid = message['jobid']
partid = message['partid']

with Part(jobid, partid):
    # The context block
    process_url(message['url'])
```


## Backend Databases

Since version 0.5, multiple backend databases are supported.

* **Dynamodb** is the default. It is suitable for jobs with relatively small part counts (less than 10,000) and easy to administer via AWS tools.
    - If the `topic_arn` is not specified, the SNS topic from the `WorkTopic` environment variable.
    - If the `table_arn` is not specified, the DynamoDB table will be determined from the `ProgressTable` environment variable.
* **Redis** requires more administration but is highly performant and scales well.
    - Can specify the `host`, `port` and `db` for the Redis connection which defaults to `localhost`, `6379` and `0` respectively.
    - If the `topic_arn` is not specified, the SNS topic from the `WorkTopic` environment variable.

These backends can be used by creating an instance of the desired class and passing it as the `progress` argument.

```python

from watchbot_progress.backends.redis import RedisProgress

p = RedisProgress(host='localhost', port=6379)

create_job(parts, progress=p)

# ... and on the processing side

with Part(partid, jobid, progress=p):
    process_url(message['url'])
```

For more information about writing a backend database, see [docs/WatchbotProgress-interface.md](docs/WatchbotProgress-interface.md)


## Command line interface

The `watchbot-progress-py` command is provided to check the status of jobs in a table.

```
$ watchbot-progress-py --help
Usage: watchbot-progress-py [OPTIONS] COMMAND [ARGS]...

  Main click command

Options:
  --help  Show this message and exit.

Commands:
  info     Returns the status of a specific jobid for a...
  ls       Scans the database for jobs and lists them as...
  pending  Streams out all pending part numbers for a...
```
