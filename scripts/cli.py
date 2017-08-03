import json
import click
import boto3
import re


def format_response(item):
    pct_done = 1.
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
    '''Scans the table and lists all pending jobs
    '''
    dynamodb = boto3.resource('dynamodb')
    db = dynamodb.Table(table)

    scan = db.scan(ConsistentRead=True)

    for s in scan['Items']:
        response = format_response(s)
        if not hide_completed or response['pct_done'] != '100.00%':
            click.echo(json.dumps(response))

if __name__ == '__main__':
    main()
