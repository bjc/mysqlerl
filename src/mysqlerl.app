%% -*- Erlang -*-
%% Copyright (C) 2008, Brian Cully

{application, mysqlerl,
 [{description, "mysqlerl"},
  {vsn, "0"},
  {modules, [mysqlerl, mysqlerl_app, mysqlerl_connection_sup,
             mysqlerl_connection, mysql_port_sup, mysql_port]},
  {registered, [mysqlerl, mysqlerl_app, mysqlerl_connection_sup]},
  {applications, [kernel, stdlib]},
  {env, []},
  {mod, {mysqlerl_app, []}}]}.
