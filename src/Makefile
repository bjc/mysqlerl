EFLAGS =	-W +debug_info

BEAMDIR =	../ebin

BINS =		$(BEAMDIR)/mysqlerl.app
BEAMS =		mysqlerl.beam mysqlerl_app.beam mysqlerl_connection.beam \
		mysqlerl_port.beam mysqlerl_port_sup.beam mysqlerl_sup.beam

all:	$(BEAMDIR) $(BINS)

clean:
	rm -rf *.beam
	rm -rf $(BINS) $(BEAMS) $(BEAMDIR)/mysqlerl.app

%.beam: %.erl
	erlc $(EFLAGS) $<

$(BEAMDIR)/mysqlerl.app: $(BEAMDIR) $(BEAMS)
	cp $(BEAMS) $(BEAMDIR)
	cp mysqlerl.app $(BEAMDIR)

$(BEAMDIR):
	mkdir -p $(BEAMDIR)
