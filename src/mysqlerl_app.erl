-module(mysqlerl_app).
-author('bjc@kublai.com').

-behavior(application).

%% Behavior callbacks.
-export([start/2, stop/1]).

start(normal, []) ->
    mysqlerl_sup:start_link().

stop([]) ->
    ok.
