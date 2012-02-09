ERLPREFIX =	/usr/local
ERLINTERFACE = 	erl_interface-3.7.6
ERLINCS =	-I$(ERLPREFIX)/lib/erlang/lib/$(ERLINTERFACE)/include
ERLLIBS =	-L$(ERLPREFIX)/lib/erlang/lib/$(ERLINTERFACE)/lib \
		-lerl_interface -lei

MYSQLPREFIX =	/usr/local/mysql
MYSQLINCS =	-I$(MYSQLPREFIX)/include
#MYSQLLIBS =	-L$(MYSQLPREFIX)/lib -lmysqlclient
MYSQLLIBS =	$(MYSQLPREFIX)/lib/libmysqlclient.a

CFLAGS =	-g -Wall -Werror $(MYSQLINCS) $(ERLINCS)
LDFLAGS =	$(MYSQLLIBS) $(ERLLIBS)

BINDIR =	../priv/bin

BINS =	$(BINDIR)/mysqlerl
OBJS =	io.o log.o msg.o mysqlerl.o

all:	$(BINS)

clean:
	rm -rf *.o $(BINS)

$(BINDIR)/mysqlerl: $(BINDIR) $(OBJS)
	$(CC) -o $@ $(OBJS) $(LDFLAGS)

$(BINDIR):
	mkdir -p $(BINDIR)
