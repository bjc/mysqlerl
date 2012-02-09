%% -*- Erlang -*-
%% Copyright (C) 2008, Brian Cully

{application, mysqlerl,
 [{description, "ODBC-compatible MYSQL driver"},
  {vsn, "0"},
  {modules, [mysqlerl, mysqlerl_app, mysqlerl_sup, mysqlerl_connection,
             mysql_port_sup, mysql_port]},
  {registered, [mysqlerl_app, mysqlerl_sup]},
  {applications, [kernel, stdlib]},
  {env, []},
  {mod, {mysqlerl_app, []}}]}.
