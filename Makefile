.PHONY:	all install test

all:
	@@cd src && make all

install:
	@@cd src && make install

test:	all
	@@cd test && make test
