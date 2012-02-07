.PHONY:	all install test

all:
	@@cd lib/src && make all

install:
	@@cd lib/src && make install

test:
	@@cd lib/test && make test
