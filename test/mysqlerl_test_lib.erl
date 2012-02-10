%%% @author Brian Cully <bjc@kublai.com>
%%% @copyright (C) 2012, Brian Cully
%%% @doc
%%%
%%% @end
%%% Created :  9 Feb 2012 by Brian Cully <bjc@kublai.com>

-module(mysqlerl_test_lib).

-compile(export_all).

mysql_cmd(undefined, undefined) ->
    "mysql";
mysql_cmd(User, undefined) ->
    io_lib:format("mysql -u'~s'", [User]);
mysql_cmd(undefined, Pass) ->
    io_lib:format("mysql -p'~s'", [Pass]);
mysql_cmd(User, Pass) ->
    io_lib:format("mysql -u'~s' -p'~s'", [User, Pass]).

create_db(User, Pass, Name) ->
    drop_db(User, Pass, Name),
    SQL = io_lib:format("CREATE DATABASE ~s", [Name]),
    CMD = mysql_cmd(User, Pass),
    os:cmd(io_lib:format("echo '~s' | ~s", [SQL, CMD])).

drop_db(User, Pass, Name) ->
    SQL = io_lib:format("DROP DATABASE IF EXISTS ~s", [Name]),
    CMD = mysql_cmd(User, Pass),
    os:cmd(io_lib:format("echo '~s' | ~s", [SQL, CMD])).

create_table(User, Pass, Name, DataDir) ->
    CMD = mysql_cmd(User, Pass),
    os:cmd(io_lib:format("~s ~s < ~s/table-data.sql", [CMD, Name, DataDir])).
