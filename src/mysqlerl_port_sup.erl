-module(mysqlerl_port_sup).
-author('bjc@kublai.com').

-behavior(supervisor).

-export([start_link/1]).
-export([init/1]).

start_link(Cmd) ->
    supervisor:start_link(?MODULE, [Cmd]).

init([Cmd]) ->
    Port = {mysqlerl_port, {mysqlerl_port, start_link, [Cmd]},
            transient, 5, worker, [mysqlerl_port]},
    {ok, {{one_for_one, 10, 5},
          [Port]}}.
