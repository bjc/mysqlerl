-module(mysqlerl_connection).
-author('bjc@kublai.com').

-include("mysqlerl.hrl").

-behavior(gen_server).

-export([start_link/7, stop/1]).

-export([init/1, terminate/2, code_change/3,
         handle_call/3, handle_cast/2, handle_info/2]).

-record(state, {port, owner}).

-define(CONNECT_TIMEOUT, 30000).

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
    erlang:monitor(process, Owner),
    Ref = open_port({spawn, helper()}, [{packet, 4}, binary]),
    ConnectArgs = #sql_connect{host     = Host,
                               port     = Port,
                               database = Database,
                               user     = User,
                               password = Password,
                               options  = Options},
    case send_port_cmd(Ref, ConnectArgs, ?CONNECT_TIMEOUT) of
        {data, ok} ->
            {ok, #state{port = Ref, owner = Owner}};
        {'EXIT', Ref, Reason} ->
            {stop, {port_closed, Reason}}
    end.

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
handle_call({Req, Timeout}, From, State) ->
    case send_port_cmd(State#state.port, Req, Timeout) of
        {data, Res} ->
            {reply, Res, State};
        {'EXIT', _Ref, Reason} ->
            {stop, {port_closed, Reason}, State};
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

handle_info({'DOWN', _Monitor, process, _Reason, _PID, Reason},
            #state{owner = PID} = State) ->
    io:format("DEBUG: owner ~p shut down: ~p.~n", [PID, Reason]),
    {stop, normal, State}.

helper() ->
    case code:priv_dir(mysqlerl) of
        PrivDir when is_list(PrivDir) -> ok;
        {error, bad_name} -> PrivDir = filename:join(["..", "priv"])
    end,
    filename:nativename(filename:join([PrivDir, "bin", "mysqlerl"])).

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
