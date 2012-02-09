-module(mysqlerl_sup).
-author('bjc@kublai.com').

-behavior(supervisor).

-export([init/1]).

init([Name]) ->
    Connection = {Name, {mysqlerl_connection, start_link, []},
                  temporary, 7000, worker, [mysqlerl_connection]},
    {ok, {{simple_one_for_one, 0, 3600}, [Connection]}}.
