%%%-------------------------------------------------------------------
%%% @author Brian Cully <bjc@kublai.com>
%%% @copyright (C) 2012, Brian Cully
%%% @doc
%%%
%%% @end
%%% Created :  6 Feb 2012 by Brian Cully <bjc@kublai.com>
%%%-------------------------------------------------------------------
-module(mysqlerl_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").

-define(Q, "SELECT username, password FROM user ORDER BY username ASC").
-define(COLS, ["username", "password"]).

%%--------------------------------------------------------------------
%% @spec suite() -> Info
%% Info = [tuple()]
%% @end
%%--------------------------------------------------------------------
suite() ->
    [{timetrap,{seconds,30}},
     {require, db_info}].

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

%%--------------------------------------------------------------------
%% @spec init_per_suite(Config0) ->
%%     Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%% @end
%%--------------------------------------------------------------------
init_per_suite(Config) ->
    DBInfo  = ct:get_config(db_info),
    DataDir = ?config(data_dir, Config),
    User    = ?config(username, DBInfo),
    Pass    = ?config(password, DBInfo),
    Name    = ?config(name, DBInfo),

    create_db(User, Pass, Name),
    create_table(User, Pass, Name, DataDir),
    ok = application:start(mysqlerl),
    Config.

%%--------------------------------------------------------------------
%% @spec end_per_suite(Config0) -> void() | {save_config,Config1}
%% Config0 = Config1 = [tuple()]
%% @end
%%--------------------------------------------------------------------
end_per_suite(_Config) ->
    DBInfo = ct:get_config(db_info),
    User   = ?config(username, DBInfo),
    Pass   = ?config(password, DBInfo),
    Name   = ?config(name, DBInfo),

    ok = application:stop(mysqlerl),
    drop_db(User, Pass, Name).

%%--------------------------------------------------------------------
%% @spec init_per_group(GroupName, Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%% GroupName = atom()
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%% @end
%%--------------------------------------------------------------------
init_per_group(_GroupName, Config) ->
    Config.

%%--------------------------------------------------------------------
%% @spec end_per_group(GroupName, Config0) ->
%%               void() | {save_config,Config1}
%% GroupName = atom()
%% Config0 = Config1 = [tuple()]
%% @end
%%--------------------------------------------------------------------
end_per_group(_GroupName, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% @spec init_per_testcase(TestCase, Config0) ->
%%               Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%% TestCase = atom()
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%% @end
%%--------------------------------------------------------------------
init_per_testcase(_TestCase, Config) ->
    DBInfo = ct:get_config(db_info),
    {ok, DBRef} = mysqlerl:connect(?config(host, DBInfo),
				   ?config(port, DBInfo),
				   ?config(name, DBInfo),
				   ?config(username, DBInfo),
				   ?config(password, DBInfo),
				   ?config(options, DBInfo)),
    [{db_ref, DBRef} | Config].

%%--------------------------------------------------------------------
%% @spec end_per_testcase(TestCase, Config0) ->
%%               void() | {save_config,Config1} | {fail,Reason}
%% TestCase = atom()
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%% @end
%%--------------------------------------------------------------------
end_per_testcase(_TestCase, Config) ->
    ok = mysqlerl:disconnect(?config(db_ref, Config)).

%%--------------------------------------------------------------------
%% @spec groups() -> [Group]
%% Group = {GroupName,Properties,GroupsAndTestCases}
%% GroupName = atom()
%% Properties = [parallel | sequence | Shuffle | {RepeatType,N}]
%% GroupsAndTestCases = [Group | {group,GroupName} | TestCase]
%% TestCase = atom()
%% Shuffle = shuffle | {shuffle,{integer(),integer(),integer()}}
%% RepeatType = repeat | repeat_until_all_ok | repeat_until_all_fail |
%%              repeat_until_any_ok | repeat_until_any_fail
%% N = integer() | forever
%% @end
%%--------------------------------------------------------------------
groups() ->
    [{all, [sequence],
      [{group, read_queries}, {group, cursor}]},
     {read_queries, [shuffle],
      [describe_table, sql_query, param_query, select_count]},
     {cursor, [shuffle],
      [first, last, next, prev,
       next_after_first, next_after_last, prev_after_first, prev_after_last,
       next_all, prev_all, next_prev_next, prev_next_prev,
       select_next, select_relative, select_absolute]},
     {trans, [sequence],
      [commit, rollback]}].

%%--------------------------------------------------------------------
%% @spec all() -> GroupsAndTestCases | {skip,Reason}
%% GroupsAndTestCases = [{group,GroupName} | TestCase]
%% GroupName = atom()
%% TestCase = atom()
%% Reason = term()
%% @end
%%--------------------------------------------------------------------
all() ->
    [{group, all}].

%%--------------------------------------------------------------------
%% @spec TestCase(Config0) ->
%%               ok | exit() | {skip,Reason} | {comment,Comment} |
%%               {save_config,Config1} | {skip_and_save,Reason,Config1}
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%% Comment = term()
%% @end
%%--------------------------------------------------------------------
app_starts(_Config) ->
    ok = application:start(mysqlerl).

app_stops(_Config) ->
    ok = application:start(mysqlerl).

describe_table(Config) ->
    io:format("describe_table ~p", [Config]),
    {ok, Description} = mysqlerl:describe_table(?config(db_ref, Config),
						"user"),
    [{"username", {sql_varchar, 20}}, {"password", {sql_varchar, 64}}] = Description.

sql_query(Config) ->
    {selected, ?COLS, Rows} = mysqlerl:sql_query(?config(db_ref, Config),
						 ?Q),
    [{"bjc", _}, {"siobain", _}] = Rows.

param_query(Config) ->
    {selected, ?COLS, Rows} = mysqlerl:param_query(?config(db_ref, Config),
						   "SELECT * FROM user WHERE username=?",
						   [{{sql_varchar, 20}, "bjc"}]),
    [{"bjc", _}] = Rows.

select_count(Config) ->
    {ok, 2} = mysqlerl:select_count(?config(db_ref, Config),
				    "SELECT username FROM user").

select_next(Config) ->
    mysqlerl:select_count(?config(db_ref, Config), ?Q),
    {selected, ?COLS, Rows} = mysqlerl:select(?config(db_ref, Config),
					      next, 1),
    [{"bjc", _}] = Rows.

select_absolute(Config) ->
    mysqlerl:select_count(?config(db_ref, Config), ?Q),
    mysqlerl:next(?config(db_ref, Config)),
    {selected, ?COLS, Rows} = mysqlerl:select(?config(db_ref, Config),
					      {absolute, 1}, 1),
    [{"bjc", _}] = Rows.

select_relative(Config) ->
    mysqlerl:select_count(?config(db_ref, Config), ?Q),
    mysqlerl:next(?config(db_ref, Config)),
    {selected, ?COLS, Rows} = mysqlerl:select(?config(db_ref, Config),
					      {relative, 1}, 1),
    [{"siobain", _}] = Rows.

first(Config) ->
    mysqlerl:select_count(?config(db_ref, Config), ?Q),
    {selected, ?COLS, Rows} = mysqlerl:first(?config(db_ref, Config)),
    [{"bjc", _}] = Rows.

last(Config) ->
    mysqlerl:select_count(?config(db_ref, Config), ?Q),
    {selected, ?COLS, Rows} = mysqlerl:last(?config(db_ref, Config)),
    [{"siobain", _}] = Rows.

next(Config) ->
    mysqlerl:select_count(?config(db_ref, Config), ?Q),
    {selected, ?COLS, Rows} = mysqlerl:next(?config(db_ref, Config)),
    [{"bjc", _}] = Rows.

next_after_first(Config) ->
    mysqlerl:select_count(?config(db_ref, Config), ?Q),
    mysqlerl:first(?config(db_ref, Config)),
    {selected, ?COLS, [{"siobain", _}]} = mysqlerl:next(?config(db_ref, Config)).

next_after_last(Config) ->
    mysqlerl:select_count(?config(db_ref, Config), ?Q),
    mysqlerl:last(?config(db_ref, Config)),
    {selected, ?COLS, []} = mysqlerl:next(?config(db_ref, Config)).

next_all(Config) ->
    mysqlerl:select_count(?config(db_ref, Config), ?Q),
    {selected, ?COLS, [{"bjc", _}]} = mysqlerl:next(?config(db_ref, Config)),
    {selected, ?COLS, [{"siobain", _}]} = mysqlerl:next(?config(db_ref, Config)),
    {selected, ?COLS, []} = mysqlerl:next(?config(db_ref, Config)).

prev(Config) ->
    mysqlerl:select_count(?config(db_ref, Config), ?Q),
    mysqlerl:last(?config(db_ref, Config)),
    {selected, ?COLS, Rows} = mysqlerl:prev(?config(db_ref, Config)),
    [{"bjc", _}] = Rows.

prev_all(Config) ->
    mysqlerl:select_count(?config(db_ref, Config), ?Q),
    mysqlerl:last(?config(db_ref, Config)),
    {selected, ?COLS, [{"bjc", _}]} = mysqlerl:prev(?config(db_ref, Config)),
    {selected, ?COLS, []} = mysqlerl:prev(?config(db_ref, Config)).

prev_after_first(Config) ->
    mysqlerl:select_count(?config(db_ref, Config), ?Q),
    mysqlerl:first(?config(db_ref, Config)),
    {selected, ?COLS, []} = mysqlerl:prev(?config(db_ref, Config)).

prev_after_last(Config) ->
    mysqlerl:select_count(?config(db_ref, Config), ?Q),
    mysqlerl:last(?config(db_ref, Config)),
    {selected, ?COLS, [{"bjc", _}]} = mysqlerl:prev(?config(db_ref, Config)).

next_prev_next(Config) ->
    mysqlerl:select_count(?config(db_ref, Config), ?Q),
    mysqlerl:first(?config(db_ref, Config)),
    {selected, ?COLS, [{"siobain", _}]} = mysqlerl:next(?config(db_ref, Config)),
    {selected, ?COLS, [{"bjc", _}]} = mysqlerl:prev(?config(db_ref, Config)),
    {selected, ?COLS, [{"siobain", _}]} = mysqlerl:next(?config(db_ref, Config)).

prev_next_prev(Config) ->
    mysqlerl:select_count(?config(db_ref, Config), ?Q),
    mysqlerl:last(?config(db_ref, Config)),
    {selected, ?COLS, [{"bjc", _}]} = mysqlerl:prev(?config(db_ref, Config)),
    {selected, ?COLS, [{"siobain", _}]} = mysqlerl:next(?config(db_ref, Config)),
    {selected, ?COLS, [{"bjc", _}]} = mysqlerl:prev(?config(db_ref, Config)).

commit(Config) ->
    ok = mysqlerl:commit(?config(db_ref, Config), commit),
    {skip, "Not implemented"}.

rollback(Config) ->
    ok = mysqlerl:rollback(?config(db_ref, Config), rollback),
    {skip, "Not implemented"}.
