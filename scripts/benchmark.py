from concurrent import futures

import click

from watchbot_progress import Part, create_job
from watchbot_progress.backends.redis import RedisProgress
from watchbot_progress.backends.dynamodb import DynamoProgress


def annotate_parts(parts, jobid):
    return [
        dict(**part, partid=partid, jobid=jobid)
        for partid, part in enumerate(parts)]


@click.command()
@click.option('--backend', '-b', type=click.Choice(['redis', 'dynamodb']),
              help="backend database", required=True)
@click.option('--number', '-n', default=100,
              help="number of parts")
def main(backend, number):
    if backend == 'redis':
        progress = RedisProgress(host='localhost', port='6379')
    elif backend == 'dynamodb':
        progress = DynamoProgress()

    parts = [dict() for _ in range(number)]
    workers = 25

    # Simulates Subject:start messages
    jobid = create_job(parts, progress=progress, workers=workers)

    # Simulates concurrent Subject:map messages
    # The annonate_parts function recreates the messages that would be recieved
    msgs = annotate_parts(parts, jobid)

    def doit(msg, jobid=jobid, progress=progress):
        partid = msg['partid']
        with Part(partid=partid, jobid=jobid, progress=progress):
            pass

    with futures.ThreadPoolExecutor(max_workers=workers) as executor:
        executor.map(doit, msgs)

    print(f"completed watchbot progress for {number} parts using {progress}")


if __name__ == "__main__":
    main()
