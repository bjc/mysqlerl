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
EFLAGS =	-W +debug_info

PRIVDIR =	../priv
BEAMDIR =	../ebin

BINS =		$(PRIVDIR)/mysqlerl $(BEAMDIR)/mysqlerl.app
MYSQLERLOBJS =	io.o log.o msg.o mysqlerl.o
BEAMS =		mysqlerl.beam mysqlerl_app.beam mysqlerl_connection.beam \
		mysqlerl_port.beam mysqlerl_port_sup.beam mysqlerl_sup.beam

all:	$(PRIVDIR) $(BEAMDIR) $(BINS)

clean:
	rm -rf *.o *.beam
	rm -rf $(BINS) $(MYSQLERLOBJS) $(BEAMS) $(BEAMDIR)/mysqlerl.app

%.beam: %.erl
	erlc $(EFLAGS) $<

$(PRIVDIR)/mysqlerl: $(PRIVDIR) $(MYSQLERLOBJS)
	$(CC) -o $@ $(MYSQLERLOBJS) $(LDFLAGS)

$(BEAMDIR)/mysqlerl.app: $(BEAMDIR) $(BEAMS)
	cp $(BEAMS) $(BEAMDIR)
	cp mysqlerl.app $(BEAMDIR)

$(PRIVDIR):
	mkdir -p $(PRIVDIR)

$(BEAMDIR):
	mkdir -p $(BEAMDIR)
