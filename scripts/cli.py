from __future__ import division

import json
import click
import boto3
import re


def format_response(item):
    '''Given a response item from a watchbot-progress style
    job record, format and return information as to the job's
    progress.

    Parameters
    -----------
    item: dynamodb dictionary
        response item representing a watchbot-progress
        job.
        Required keys: "total", "id"
        Optional keys: "metadata", "parts"

    Returns
    --------
    metadata: dictionary
        formatted status information
        ```
        {
            'id': <job id>,
            'total_parts': <[int] total number of parts>,
            'parts_done': <[int] number of completed parts>,
            'pct_done': <[str] human-readable percent complete>,
            'metadata': <[dict] any job metadata>
        }
        ```
    '''
    total_parts = int(item['total'])
    metadata = item.get('metadata', {})
    pending_parts = item.get('parts', [])
    parts_done = (total_parts - len([int(i) for i in pending_parts]))

    return {
        'id': item['id'],
        'total_parts': total_parts,
        'parts_done': parts_done,
        'pct_done': '{:.2f}%'.format(parts_done / total_parts * 100),
        'metadata': metadata
    }


@click.group()
def main():
    pass

@main.command()
@click.argument('table', type=str)
@click.argument('jobid', type=str)
def info(table, jobid):
    '''Returns the progress of a specific jobid
    for a watchbot-progress job
    '''
    dynamodb = boto3.resource('dynamodb')
    db = dynamodb.Table(table)

    res = db.get_item(Key={'id': jobid}, ConsistentRead=True)

    if 'Error' in res or 'Item' not in res:
        raise

    jobinfo = format_response(res['Item'])

    click.echo(json.dumps(jobinfo))


@main.command()
@click.argument('table', type=str)
@click.option('--hide-completed', is_flag=True)
def ls(table, hide_completed):
    '''Scans the given watchbot-progress table
    and returns their individual progress info
    '''
    dynamodb = boto3.resource('dynamodb')
    db = dynamodb.Table(table)

    scan = db.scan(ConsistentRead=True)

    for s in scan['Items']:
        response = format_response(s)
        if (not hide_completed or response['pct_done'] != '100.00%'):
            click.echo(json.dumps(response))


@main.command()
@click.argument('table', type=str)
@click.argument('jobid', type=str)
def pending(table, jobid):
    '''Streams out all pending
    part numbers for a given jobid
    '''
    dynamodb = boto3.resource('dynamodb')
    db = dynamodb.Table(table)

    res = db.get_item(Key={'id': jobid}, ConsistentRead=True)

    if 'Error' in res or 'Item' not in res:
        raise

    pending = res['Item'].get('parts', [])

    if len(pending) == 0:
        click.echo('No parts remaining in {jobid}'.format(jobid=jobid))

    for p in pending:
        click.echo(int(p))


if __name__ == '__main__':
    main()
