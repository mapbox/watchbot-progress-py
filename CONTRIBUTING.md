
# Development

### Build requirements

Create and activate a virtualenv for watchbot-progress-py.

then:

```sh
$ pip install -U pip
$ pip install -r requirements-dev.txt
```

### Installing watchbot-progress-py

watchbot-progress-py and it's dev and test dependencies can be installed with:

```sh
$ pip install -e .[test]
```

### Running the tests

Tests can be run locally with:

```sh
tox
```

### Publishing a release

In order to publish a [watchbot-progress-py release](https://pypi.org/project/watchbot-progress) to PyPI, you'll need access to a Maintainer account.

```sh
python setup.py sdist bdist_wheel --universal
twine upload dist/*
```
