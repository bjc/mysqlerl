-module(mysqlerl_connection_sup).
-author('bjc@kublai.com').

-behavior(supervisor).

-export([start_link/0, connect/6, random_child/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

connect(Host, Port, Database, User, Password, Options) ->
    supervisor:start_child(?MODULE, [self(), Host, Port, Database,
                                     User, Password, Options]).

random_child() ->
    case get_pids() of
        []   -> {error, no_connections};
        Pids -> lists:nth(erlang:phash(now(), length(Pids)), Pids)
    end.

init([]) ->
    Connection = {undefined, {mysqlerl_connection, start_link, []},
                  transient, 5, worker, [mysqlerl_connection]},
    {ok, {{simple_one_for_one, 10, 5},
          [Connection]}}.

get_pids() ->
    [Pid || {_Id, Pid, _Type, _Modules} <- supervisor:which_children(?MODULE)].
