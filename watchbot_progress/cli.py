from __future__ import division

import json
import click
try:  # pragma: no cover
    from urllib.parse import urlparse
except ImportError:  # pragma: no cover
    from urlparse import urlparse

from watchbot_progress.backends.redis import RedisProgress
from watchbot_progress.backends.dynamodb import DynamoProgress


DBHELP = 'a dynamodb table ARN or a redis URI connection string e.g. `redis://localhost:6379`'


def validate_db(ctx, param, value):
    topic_arn = 'should-never-need-to-use-SNS'

    if value.startswith('redis'):
        url = urlparse(value)
        assert url.scheme == 'redis'

        db = 0
        if url.query:
            queries = url.query.split('&')
            for query in queries:
                k, v = query.split('=')
                if k == 'db':
                    db = int(v)

        host, port = url.netloc.split(":")
        return RedisProgress(host=host, port=port, db=db, topic_arn=topic_arn)

    elif value.startswith('arn:'):
        return DynamoProgress(table_arn=value, topic_arn=topic_arn)

    else:
        raise click.BadParameter('Needs to be {}'.format(DBHELP))


@click.group()
def main():
    """Main click command
    """


@main.command()
@click.argument('jobid', type=str)
@click.option('--database', '-d', default='dynamodb', nargs=1, callback=validate_db, help=DBHELP)
def info(jobid, database):
    '''Returns the status of a specific jobid for a watchbot-progress job
    as single JSON object
    '''
    status = database.status(jobid)
    click.echo(json.dumps(status))


@main.command()
@click.option('--database', '-d', default='dynamodb', nargs=1, callback=validate_db, help=DBHELP)
@click.option('--status/--jobid', default=True,
              help="Show full status object, otherwise only jobid")
@click.option('--hide-completed', is_flag=True,
              help="show only jobs with pending parts")
def ls(database, status, hide_completed):
    '''Scans the database for jobs and lists them as a jobids
    '''
    jobs = database.list_jobs(status=status)
    for job in jobs:
        if not hide_completed or (hide_completed and job['remaining'] > 0):
            click.echo(job)


@main.command()
@click.argument('jobid', type=str)
@click.option('--database', '-d', default='dynamodb', nargs=1, callback=validate_db, help=DBHELP)
@click.option('--array', is_flag=True,
              help='Output as JSON array [Default: line-delimited]')
def pending(jobid, database, array):
    '''Streams out all pending part numbers for a given jobid
    '''
    parts = database.list_pending_parts(jobid)
    if array:
        click.echo(json.dumps(list(parts)))
    else:
        for part in parts:
            click.echo(part)
