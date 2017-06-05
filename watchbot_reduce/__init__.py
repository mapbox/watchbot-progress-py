#!/usr/bin/env python
import json
import os
import uuid


class ProgressTable:
    # https://github.com/mapbox/watchbot-progress#reporting-progress-in-javascript
    def __init__(self, jobid, table):
        self.jobid = jobid
        self.table = table
        self.file = f'/tmp/{table}_{jobid}.txt'
        self.total = None

    @property
    def failed(self):
        with open(self.file, 'r') as fh:
            for line in fh.readlines():
                if 'failed' in line:
                    return True
                    break
        return False

    def get_total(self):
        with open(self.file, 'r') as fh:
            for line in fh.readlines():
                if 'Total' in line:
                    return int(line.strip().replace('Total: ', ''))
                    break
        return 0

    def set_total(self, total):
        self.total = total
        with open(self.file, 'w+') as fh:
            fh.write(f'Total: {self.total}')

    def complete_part(self, partid):
        with open(self.file, 'w+') as fh:
            fh.write(f'Completed part {partid}')

        return True  # TODO is this job completely done?

    def set_metadata(self, **kwargs):
        for k, v in kwargs.items():
            print(f'{k}: {v}')

    def status(self):
        return {"progress": 0.25}

    def fail_job(self, reason):
        with open(self.file, 'w+') as fh:
            fh.write(f'Job failed: {reason}')


class WorkTopic:
    def __init__(self, topic_arn):
        self.topic_arn = topic_arn
        self.file = f'/tmp/{topic}.txt'

    def publish(self, subject, message):
        with open(self.file, 'w+') as fh:
            fh.write(f'{Subject}: {Message}\n')


class Task:
    def __init__(self):
        self.subject = os.environ['Subject']
        self.message = json.loads(os.environ['Message'])

        self.topic_arn = os.environ['WorkTopic']
        self.topic = WorkTopic(self.topic_arn)

        self.table = os.environ['ProgressTable']  # set by watchbot in reduce mode
        self.progress = None

    def start(self):
        """Starts a watchbot reduce job
        """
        # Parse message and determine how many parts we have
        # parts = split(self.message['something'])
        parts = list(range(100))
        jobid = uuid.uuid4()
        self.progress = ProgressTable(jobid, self.table)

        # Set the total parts for the job in the database
        self.progress.set_total = len(parts)

        # Publish SNS messages for each part, passing along the job id and part number
        for part in parts:
            params = {
                'TopicArn': self.topic_arn,
                'Subject': 'part',
                'Message': json.dumps({'partid': part, 'jobid': jobid})}
            print(json.dumps(params))

    def do_part(self):
        """do the work, and some accounting

        * If processing fails, set the job as failed
        """
        jobid = self.message['jobid']
        partid = self.message['partid']
        self.progress = ProgressTable(jobid, self.table)

        # Check status of the overall job (quit if failed)
        if self.progress.failed:
            print("Skipping {partid}; job {jobid} failed")

        # Do the work
        try:
            print(self.message)
        except:
            self.progress.fail_job()

        # Set the part as complete
        all_done = self.progress.complete_part(partid)

        # If entire job is complete, send an SNS message to fire off the reduce step
        if all_done:
            params = {
                'TopicArn': self.topic_arn,
                'Subject': 'reduce',
                'Message': json.dumps({'jobid': self.jobid})}
            print(json.dumps(params))

    def finish(self):
        """Wrap up the job
        """
        print(self.jobid)


def worker():
    task = Task()
    if task.subject == 'start-job':
        task.start()
    elif task.subject == 'part':
        task.do_part()
    elif task.subject == 'reduce':
        task.finish()
    else:
        raise ValueError("Bad subject")


if __name__ == "__main__":
    worker()
