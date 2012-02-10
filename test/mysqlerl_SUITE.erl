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

    mysqlerl_test_lib:create_db(User, Pass, Name),
    mysqlerl_test_lib:create_table(User, Pass, Name, DataDir),
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
    mysqlerl_test_lib:drop_db(User, Pass, Name).

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
      [{group, read_queries}, {group, cursor},
       {group, trans}, {group, errors}]},
     {read_queries, [shuffle],
      [describe_table, sql_query, param_query, select_count]},
     {cursor, [shuffle],
      [first, last, next, prev,
       next_after_first, next_after_last, prev_after_first, prev_after_last,
       next_all, prev_all, next_prev_next, prev_next_prev,
       select_next, select_relative, select_absolute]},
     {trans, [sequence],
      [commit, rollback]},
     {errors, [shuffle],
      [select_no_results, first_no_results, last_no_results,
       next_no_results, prev_no_results]}].

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
describe_table(doc) ->
    ["Tests describe_table/2 for varchar columns."];
describe_table(Config) ->
    {ok, Description} = mysqlerl:describe_table(?config(db_ref, Config),
                                                "user"),
    [{"username", {sql_varchar, 20}}, {"password", {sql_varchar, 64}}] = Description.

sql_query(doc) ->
    ["Tests sql_query/2 for sample data."];
sql_query(Config) ->
    {selected, ?COLS, Rows} = mysqlerl:sql_query(?config(db_ref, Config),
                                                 ?Q),
    [{"bjc", _}, {"siobain", _}] = Rows.

param_query(doc) ->
    ["Tests param_query/3 for sample data."];
param_query(Config) ->
    {selected, ?COLS, Rows} = mysqlerl:param_query(?config(db_ref, Config),
                                                   "SELECT * FROM user WHERE username=?",
                                                   [{{sql_varchar, 20}, "bjc"}]),
    [{"bjc", _}] = Rows.

select_count(doc) ->
    ["Tests select_count/2 for sample data."];
select_count(Config) ->
    {ok, 2} = mysqlerl:select_count(?config(db_ref, Config),
                                    "SELECT username FROM user").

select_next(doc) ->
    ["Tests select/3 with next parameter."];
select_next(Config) ->
    mysqlerl:select_count(?config(db_ref, Config), ?Q),
    {selected, ?COLS, Rows} = mysqlerl:select(?config(db_ref, Config),
                                              next, 1),
    [{"bjc", _}] = Rows.

select_absolute(doc) ->
    ["Tests select/3 with absolute position."];
select_absolute(Config) ->
    mysqlerl:select_count(?config(db_ref, Config), ?Q),
    mysqlerl:next(?config(db_ref, Config)),
    {selected, ?COLS, Rows} = mysqlerl:select(?config(db_ref, Config),
                                              {absolute, 1}, 1),
    [{"bjc", _}] = Rows.

select_relative(doc) ->
    ["Tests select/3 with relative position."];
select_relative(Config) ->
    mysqlerl:select_count(?config(db_ref, Config), ?Q),
    mysqlerl:next(?config(db_ref, Config)),
    {selected, ?COLS, Rows} = mysqlerl:select(?config(db_ref, Config),
                                              {relative, 1}, 1),
    [{"siobain", _}] = Rows.

first(doc) ->
    ["Tests first/1 from initial result set."];
first(Config) ->
    mysqlerl:select_count(?config(db_ref, Config), ?Q),
    {selected, ?COLS, Rows} = mysqlerl:first(?config(db_ref, Config)),
    [{"bjc", _}] = Rows.

last(doc) ->
    ["Tests last/1 from initial result set."];
last(Config) ->
    mysqlerl:select_count(?config(db_ref, Config), ?Q),
    {selected, ?COLS, Rows} = mysqlerl:last(?config(db_ref, Config)),
    [{"siobain", _}] = Rows.

next(doc) ->
    ["Tests next/1 from initial result set."];
next(Config) ->
    mysqlerl:select_count(?config(db_ref, Config), ?Q),
    {selected, ?COLS, Rows} = mysqlerl:next(?config(db_ref, Config)),
    [{"bjc", _}] = Rows.

next_after_first(doc) ->
    ["Tests next/1 after calling first."];
next_after_first(Config) ->
    mysqlerl:select_count(?config(db_ref, Config), ?Q),
    mysqlerl:first(?config(db_ref, Config)),
    {selected, ?COLS, [{"siobain", _}]} = mysqlerl:next(?config(db_ref, Config)).

next_after_last(doc) ->
    ["Tests next/1 after calling last."];
next_after_last(Config) ->
    mysqlerl:select_count(?config(db_ref, Config), ?Q),
    mysqlerl:last(?config(db_ref, Config)),
    {selected, ?COLS, []} = mysqlerl:next(?config(db_ref, Config)).

next_all(doc) ->
    ["Tests traversing all results with next/1."];
next_all(Config) ->
    mysqlerl:select_count(?config(db_ref, Config), ?Q),
    {selected, ?COLS, [{"bjc", _}]} = mysqlerl:next(?config(db_ref, Config)),
    {selected, ?COLS, [{"siobain", _}]} = mysqlerl:next(?config(db_ref, Config)),
    {selected, ?COLS, []} = mysqlerl:next(?config(db_ref, Config)).

prev(doc) ->
    ["Tests prev/1 after calling last/1."];
prev(Config) ->
    mysqlerl:select_count(?config(db_ref, Config), ?Q),
    mysqlerl:last(?config(db_ref, Config)),
    {selected, ?COLS, Rows} = mysqlerl:prev(?config(db_ref, Config)),
    [{"bjc", _}] = Rows.

prev_all(doc) ->
    ["Tests traversing all results with prev/1."];
prev_all(Config) ->
    mysqlerl:select_count(?config(db_ref, Config), ?Q),
    mysqlerl:last(?config(db_ref, Config)),
    {selected, ?COLS, [{"bjc", _}]} = mysqlerl:prev(?config(db_ref, Config)),
    {selected, ?COLS, []} = mysqlerl:prev(?config(db_ref, Config)).

prev_after_first(doc) ->
    ["Tests prev/1 after calling first/1."];
prev_after_first(Config) ->
    mysqlerl:select_count(?config(db_ref, Config), ?Q),
    mysqlerl:first(?config(db_ref, Config)),
    {selected, ?COLS, []} = mysqlerl:prev(?config(db_ref, Config)).

prev_after_last(doc) ->
    ["Tests prev/1 after calling last/1."];
prev_after_last(Config) ->
    mysqlerl:select_count(?config(db_ref, Config), ?Q),
    mysqlerl:last(?config(db_ref, Config)),
    {selected, ?COLS, [{"bjc", _}]} = mysqlerl:prev(?config(db_ref, Config)).

next_prev_next(doc) ->
    ["Tests that calling next/1, then prev/1, then next/1 moves the cursor properly."];
next_prev_next(Config) ->
    mysqlerl:select_count(?config(db_ref, Config), ?Q),
    mysqlerl:first(?config(db_ref, Config)),
    {selected, ?COLS, [{"siobain", _}]} = mysqlerl:next(?config(db_ref, Config)),
    {selected, ?COLS, [{"bjc", _}]} = mysqlerl:prev(?config(db_ref, Config)),
    {selected, ?COLS, [{"siobain", _}]} = mysqlerl:next(?config(db_ref, Config)).

prev_next_prev(doc) ->
    ["Tests that calling prev/1, then next/1, then prev/1 moves the cursor properly."];
prev_next_prev(Config) ->
    mysqlerl:select_count(?config(db_ref, Config), ?Q),
    mysqlerl:last(?config(db_ref, Config)),
    {selected, ?COLS, [{"bjc", _}]} = mysqlerl:prev(?config(db_ref, Config)),
    {selected, ?COLS, [{"siobain", _}]} = mysqlerl:next(?config(db_ref, Config)),
    {selected, ?COLS, [{"bjc", _}]} = mysqlerl:prev(?config(db_ref, Config)).

commit(doc) ->
    ["Tests that commit/1 with commit commits pending transactions."];
commit(Config) ->
    {updated, 0} = mysqlerl:commit(?config(db_ref, Config), commit),
    {skip, "Not implemented"}.

rollback(doc) ->
    ["Tests that rollback/1 with rollback undoes pending transactions."];
rollback(Config) ->
    {updated, 0} = mysqlerl:commit(?config(db_ref, Config), rollback),
    {skip, "Not implemented"}.

describe_no_table(doc) ->
    ["Test that describe_table/2 fails properly when no table exists."];
describe_no_table(Config) ->
    {error, _} = mysqlerl:describe_table(?config(db_ref, Config), "none").


select_no_results(doc) ->
    ["Tests that select/3 fails properly when no results exist."];
select_no_results(Config) ->
    {error, result_set_does_not_exist} = mysqlerl:select(?config(db_ref, Config),
                                                         next, 1).

first_no_results(doc) ->
    ["Tests that first/1 fails properly when no results exist."];
first_no_results(Config) ->
    {error, result_set_does_not_exist} = mysqlerl:first(?config(db_ref, Config)).

last_no_results(doc) ->
    ["Tests that last/1 fails properly when no results exist."];
last_no_results(Config) ->
    {error, result_set_does_not_exist} = mysqlerl:last(?config(db_ref, Config)).

next_no_results(doc) ->
    ["Tests that next/1 fails properly when no results exist."];
next_no_results(Config) ->
    {error, result_set_does_not_exist} = mysqlerl:next(?config(db_ref, Config)).

prev_no_results(doc) ->
    ["Tests that prev/1 fails properly when no results exist."];
prev_no_results(Config) ->
    {error, result_set_does_not_exist} = mysqlerl:prev(?config(db_ref, Config)).
