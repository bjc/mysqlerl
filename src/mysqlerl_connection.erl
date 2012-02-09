-module(mysqlerl_connection).
-author('bjc@kublai.com').

-include("mysqlerl.hrl").
-include("mysqlerl_port.hrl").

-behavior(gen_server).

-export([start_link/7, stop/1]).

-export([init/1, terminate/2, code_change/3,
         handle_call/3, handle_cast/2, handle_info/2]).

-record(state, {sup, owner}).

start_link(Owner, Host, Port, Database, User, Password, Options) ->
    gen_server:start_link(?MODULE, [Owner, Host, Port, Database,
                                    User, Password, Options], []).

stop(Pid) ->
    case (catch gen_server:call(Pid, stop)) of
        {'EXIT', _} -> ok;
        Other       -> Other
    end.

init([Owner, Host, Port, Database, User, Password, Options]) ->
    process_flag(trap_exit, true),
    link(Owner),
    {ok, Sup} = supervisor:start_link(mysqlerl_port_sup,
                                      [helper(), Host, Port, Database,
                                       User, Password, Options]),
    {ok, #state{sup = Sup, owner = Owner}}.

terminate(Reason, _State) ->
    io:format("DEBUG: connection got terminate: ~p~n", [Reason]),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

handle_call(Request, From, #state{owner = Owner} = State)
  when Owner /= element(1, From) ->
    error_logger:warning_msg("Request from ~p (owner: ~p): ~p",
                             [element(1, From), Owner, Request]),
    {reply, {error, process_not_owner_of_odbc_connection}, State};
handle_call(stop, _From, State) ->
    {stop, normal, State};
handle_call(Request, _From, State) ->
    {reply, gen_server:call(port_ref(State#state.sup),
                            #req{request = Request}, infinity), State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, _Reason}, #state{owner = Pid} = State) ->
    io:format("DEBUG: owner ~p shut down.~n", [Pid]),
    {stop, normal, State}.

helper() ->
    case code:priv_dir(mysqlerl) of
        PrivDir when is_list(PrivDir) -> ok;
        {error, bad_name} -> PrivDir = filename:join(["..", "priv"])
    end,
    filename:nativename(filename:join([PrivDir, "bin", "mysqlerl"])).

port_ref(Sup) ->
    [{mysqlerl_port, Ref, worker, _}] = supervisor:which_children(Sup),
    Ref.
