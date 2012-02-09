.PHONY:	all install test clean

all:
	@cd c_src && make all
	@cd src && make all

install:
	@cd src && make install

test:	all
	@cd test && make test

clean:
	@cd c_src && make clean
	@cd src && make clean
	@cd test && make clean
