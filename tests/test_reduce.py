from watchbot_reduce import ProgressTable


def test_basic():
    jobid = 123
    table = 'testTableArn'
    pt = ProgressTable(jobid, table)
    assert pt
