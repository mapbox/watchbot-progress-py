from mock import patch

import pytest
import click
from click.testing import CliRunner
from mockredis import mock_strict_redis_client

from watchbot_progress import cli


parts = (
    {'source': 'a.tif'},
    {'source': 'b.tif'},
    {'source': 'c.tif'})


@patch('watchbot_progress.cli.RedisProgress')
def test_ls(Progress, monkeypatch):
    Progress.return_value.list_jobs.return_value = ['job1', 'job2']

    runner = CliRunner()
    result = runner.invoke(cli.ls, '--jobid --database redis://localhost:6379?db=0'.split(' '))

    assert result.exit_code == 0
    assert result.output == 'job1\njob2\n'


@patch('watchbot_progress.cli.RedisProgress')
def test_info(Progress, monkeypatch):
    Progress.return_value.status.return_value = {'fake': True}

    runner = CliRunner()
    result = runner.invoke(cli.info, 'job1 --database redis://localhost:6379?db=0'.split(' '))

    Progress.assert_called()
    assert result.exit_code == 0
    assert result.output == '{"fake": true}\n'


@patch('watchbot_progress.cli.RedisProgress')
def test_pending(Progress, monkeypatch):
    Progress.return_value.list_pending_parts.return_value = [2, 0, 1]

    runner = CliRunner()
    result = runner.invoke(cli.pending, 'job1 --database redis://localhost:6379?db=0'.split(' '))

    assert result.exit_code == 0
    assert result.output == '2\n0\n1\n'


@patch('watchbot_progress.cli.RedisProgress')
def test_pending_array(Progress, monkeypatch):
    Progress.return_value.list_pending_parts.return_value = [2, 0, 1]

    runner = CliRunner()
    result = runner.invoke(
        cli.pending, 'job1 --array --database redis://localhost:6379?db=0'.split(' '))

    assert result.exit_code == 0
    assert result.output == '[2, 0, 1]\n'


def test_validate_redis():
    from watchbot_progress.backends.redis import RedisProgress
    p = cli.validate_db(None, None, 'redis://localhost:6379')
    assert isinstance(p, RedisProgress)


def test_validate_dynamo():
    from watchbot_progress.backends.dynamodb import DynamoProgress
    p = cli.validate_db(None, None, 'arn:whatevs')
    assert isinstance(p, DynamoProgress)


def test_validate_invalid():
    with pytest.raises(click.BadParameter):
        cli.validate_db(None, None, 'mysql://wat')
