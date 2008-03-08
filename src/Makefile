ERLINTERFACE = 	erl_interface-3.5.5.3

CFLAGS =	-I/usr/local/mysql/include \
		-I$(HOME)/lib/erlang/lib/erl_interface-$(ERLINTERFACE)/include \
		-O2 -g -Wall -Werror
LDFLAGS =	-L/usr/local/mysql/lib \
		-L$(HOME)/lib/erlang/lib/erl_interface-$(ERLINTERFACE)/lib \
		-lerl_interface -lei
EFLAGS =	-W +debug_info

PRIVDIR =	../priv
BEAMDIR =	../ebin

BINS =		$(PRIVDIR)/mysqlerl $(BEAMDIR)/mysqlerl.app
MYSQLERLOBJS =	io.o log.o msg.o mysqlerl.o
BEAMS =		mysqlerl.beam mysqlerl_app.beam mysqlerl_connection.beam \
		mysqlerl_connection_sup.beam
LIBS =		-lmysqlclient

all:	$(PRIVDIR) $(BEAMDIR) $(BINS)

clean:
	rm -rf *.o *.beam
	rm -rf $(BINS) $(MYSQLERLOBJS) $(BEAMS) $(BEAMDIR)/mysqlerl.app

%.beam: %.erl
	erlc $(EFLAGS) $<

$(PRIVDIR)/mysqlerl: $(PRIVDIR) $(MYSQLERLOBJS)
	$(CC) -o $@ $(LDFLAGS) $(MYSQLERLOBJS) $(LIBS)

$(BEAMDIR)/mysqlerl.app: $(BEAMDIR) $(BEAMS)
	cp $(BEAMS) $(BEAMDIR)
	cp mysqlerl.app $(BEAMDIR)

$(PRIVDIR):
	mkdir -p $(PRIVDIR)

$(BEAMDIR):
	mkdir -p $(BEAMDIR)