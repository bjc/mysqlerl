-module(mysqlerl_connection).
-author('bjc@kublai.com').

-behavior(gen_server).

-export([start_link/6, stop/1, sql_query/3, testmsg/1]).

-export([init/1, terminate/2, code_change/3,
         handle_call/3, handle_cast/2, handle_info/2]).

-record(state, {ref}).
-record(port_closed, {reason}).
-record(sql_query, {q}).

start_link(Host, Port, Database, User, Password, Options) ->
    gen_server:start_link(?MODULE, [Host, Port, Database,
                                    User, Password, Options], []).

stop(Pid) ->
    gen_server:cast(Pid, stop).

sql_query(Pid, Query, Timeout) ->
    gen_server:call(Pid, #sql_query{q = Query}, Timeout).

testmsg(Pid) ->
    gen_server:call(Pid, {test, "SELECT COUNT(*) FROM user;"}).

init([Host, Port, Database, User, Password, Options]) ->
    process_flag(trap_exit, true),
    Cmd = lists:flatten(io_lib:format("~s ~s ~w ~s ~s ~s ~s",
                                      [helper(), Host, Port, Database,
                                       User, Password, Options])),
    Ref = open_port({spawn, Cmd}, [{packet, 4}]),
    {ok, #state{ref = Ref}}.

terminate(#port_closed{reason = Reason}, #state{ref = Ref}) ->
    io:format("DEBUG: mysqlerl connection ~p shutting down (~p).~n",
              [Ref, Reason]),
    ok;
terminate(Reason, State) ->
    port_close(State#state.ref),
    io:format("DEBUG: got terminate: ~p~n", [Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_call({test, Str}, _From, #state{ref = Ref} = State) ->
    io:format("DEBUG: got test message: ~p~n", [Str]),
    port_command(Ref, Str),
    receive
        {Ref, {data, Res}} ->
            {reply, {ok, Res}, State};
        Other ->
            error_logger:warning_msg("Got unknown message: ~p~n", [Other])
    end;
handle_call(Request, From, State) ->
    io:format("DEBUG: got unknown call from ~p: ~p~n", [From, Request]),
    {noreply, State}.

handle_cast(stop, State) ->
    {stop, normal, State}.

handle_info({'EXIT', _Ref, Reason}, State) ->
    {stop, #port_closed{reason = Reason}, State};
handle_info(Info, State) ->
    io:format("DEBUG: got unknown info: ~p~n", [Info]),
    {noreply, State}.

helper() ->
    case code:priv_dir(mysqlerl) of
        PrivDir when is_list(PrivDir) -> ok;
        {error, bad_name} -> PrivDir = filename:join(["..", "priv"])
    end,
    filename:join([PrivDir, "mysqlerl"]).
