export LC_ALL=en_US.UTF-8
export LANG=en_US.UTF-8

DUMMY: test

test:
	pytest -s tests/
