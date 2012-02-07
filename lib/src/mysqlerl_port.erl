-module(mysqlerl_port).
-author('bjc@kublai.com').

-include("mysqlerl.hrl").
-include("mysqlerl_port.hrl").

-behavior(gen_server).

-export([start_link/7]).
-export([init/1, terminate/2, code_change/3,
         handle_call/3, handle_cast/2, handle_info/2]).

-define(CONNECT_TIMEOUT, 30000).

-record(state, {ref}).
-record(port_closed, {reason}).

start_link(Cmd, Host, Port, Database, User, Password, Options) ->
    gen_server:start_link(?MODULE,
                          [Cmd, Host, Port, Database, User, Password, Options],
                          []).

init([Cmd, Host, Port, Database, User, Password, Options]) ->
    process_flag(trap_exit, true),
    Ref = open_port({spawn, Cmd}, [{packet, 4}, binary]),
    {data, ok} = send_port_cmd(Ref, #sql_connect{host     = Host,
                                                 port     = Port,
                                                 database = Database,
                                                 user     = User,
                                                 password = Password,
                                                 options  = Options},
                               ?CONNECT_TIMEOUT),
    {ok, #state{ref = Ref}}.

terminate(#port_closed{reason = Reason}, #state{ref = Ref}) ->
    io:format("DEBUG: mysqlerl connection ~p shutting down (~p).~n",
              [Ref, Reason]),
    ok;
terminate(Reason, State) ->
    catch port_close(State#state.ref),
    io:format("DEBUG: mysqlerl_port got terminate: ~p~n", [Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_call(#req{request = {Request, Timeout}}, From,
            #state{ref = Ref} = State) ->
    case send_port_cmd(Ref, Request, Timeout) of
        {data, Res} ->
            {reply, Res, State};
        {'EXIT', Ref, Reason} ->
            gen_server:reply(From, {error, connection_closed}),
            {stop, #port_closed{reason = Reason}, State};
        timeout ->
            gen_server:reply(From, timeout),
            {stop, timeout, State};
        Other ->
            error_logger:warning_msg("Got unknown query response: ~p~n",
                                     [Other]),
            gen_server:reply(From, {error, connection_closed}),
            {stop, {unknownreply, Other}, State}
    end.


handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({'EXIT', Ref, Reason}, #state{ref = Ref} = State) ->
    io:format("DEBUG: Port ~p closed on ~p.~n", [Ref, State]),
    {stop, #port_closed{reason = Reason}, State}.


send_port_cmd(Ref, Request, Timeout) ->
    io:format("DEBUG: Sending request: ~p~n", [Request]),
    port_command(Ref, term_to_binary(Request)),
    receive
        {Ref, {data, Res}} ->
            {data, binary_to_term(Res)};
        Other -> Other
    after Timeout ->
            timeout
    end.
