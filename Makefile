export LC_ALL=en_US.UTF-8
export LANG=en_US.UTF-8
export PYTHONPATH=.

DUMMY: test

test:
	pytest -s tests/dags/operators/
