-module(mysqlerl_sup).
-author('bjc@kublai.com').

-behavior(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    ConnectionSup = {mysqlerl_connection_sup,
                     {mysqlerl_connection_sup, start_link, []},
                     permanent, infinity, supervisor,
                     [mysqlerl_connection_sup]},
    {ok, {{one_for_one, 10, 5}, [ConnectionSup]}}.
