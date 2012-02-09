-module(mysqlerl_app).
-author('bjc@kublai.com').

-behavior(application).

%% Behavior callbacks.
-export([start/2, stop/1]).

start(normal, Name) ->
    supervisor:start_link({local, mysqlerl_sup}, mysqlerl_sup, [Name]).

stop([]) ->
    ok.
