-module(mysqlerl_connection).
-author('bjc@kublai.com').

-include("mysqlerl.hrl").

-behavior(gen_server).

-export([start_link/7, stop/1]).

-export([init/1, terminate/2, code_change/3,
         handle_call/3, handle_cast/2, handle_info/2]).

-record(state, {ref, owner}).
-record(port_closed, {reason}).

start_link(Owner, Host, Port, Database, User, Password, Options) ->
    gen_server:start_link(?MODULE, [Owner, Host, Port, Database,
                                    User, Password, Options], []).

stop(Pid) ->
    gen_server:cast(Pid, stop).

init([Owner, Host, Port, Database, User, Password, Options]) ->
    process_flag(trap_exit, true),
    Cmd = lists:flatten(io_lib:format("~s ~s ~w ~s ~s ~s ~s",
                                      [helper(), Host, Port, Database,
                                       User, Password, Options])),
    Ref = open_port({spawn, Cmd}, [{packet, 4}, binary]),
    {ok, #state{ref = Ref, owner = Owner}}.

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

handle_call(Request, _From, State) ->
    {reply, make_request(State#state.ref, Request), State}.

handle_cast(stop, State) ->
    {stop, normal, State}.

handle_info({'EXIT', _Ref, Reason}, State) ->
    {stop, #port_closed{reason = Reason}, State}.

helper() ->
    case code:priv_dir(mysqlerl) of
        PrivDir when is_list(PrivDir) -> ok;
        {error, bad_name} -> PrivDir = filename:join(["..", "priv"])
    end,
    filename:join([PrivDir, "mysqlerl"]).

make_request(Ref, Req) ->
    io:format("DEBUG: Sending request: ~p~n", [Req]),
    port_command(Ref, term_to_binary(Req)),
    receive
        {Ref, {data, Res}} -> binary_to_term(Res);
        Other ->
            error_logger:warning_msg("Got unknown query response: ~p~n",
                                     [Other]),
            exit({badreply, Other})
    end.
