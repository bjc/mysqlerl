%%%-------------------------------------------------------------------
%%% @author Brian Cully <bjc@kublai.com>
%%% @copyright (C) 2012, Brian Cully
%%% @doc
%%%
%%% @end
%%% Created :  9 Feb 2012 by Brian Cully <bjc@kublai.com>
%%%-------------------------------------------------------------------
-module(mysqlerl_writequery_SUITE).

-compile(export_all).

-include_lib("common_test/include/ct.hrl").

%%--------------------------------------------------------------------
%% @spec suite() -> Info
%% Info = [tuple()]
%% @end
%%--------------------------------------------------------------------
suite() ->
    [{timetrap,{seconds,30}}].

%%--------------------------------------------------------------------
%% @spec init_per_suite(Config0) ->
%%     Config1 | {skip,Reason} | {skip_and_save,Reason,Config1}
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%% @end
%%--------------------------------------------------------------------
init_per_suite(Config) ->
    ok = mysqlerl_test_lib:create_db(Config),
    ok = mysqlerl_test_lib:create_table(Config),
    ok = application:start(mysqlerl),
    Config.

%%--------------------------------------------------------------------
%% @spec end_per_suite(Config0) -> void() | {save_config,Config1}
%% Config0 = Config1 = [tuple()]
%% @end
%%--------------------------------------------------------------------
end_per_suite(Config) ->
    ok = application:stop(mysqlerl),
    ok = mysqlerl_test_lib:drop_db(Config).

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
    [].

%%--------------------------------------------------------------------
%% @spec all() -> GroupsAndTestCases | {skip,Reason}
%% GroupsAndTestCases = [{group,GroupName} | TestCase]
%% GroupName = atom()
%% TestCase = atom()
%% Reason = term()
%% @end
%%--------------------------------------------------------------------
all() ->
    [insert_query, insert_param_query,
     update_query, update_param_query,
     delete_query, delete_param_query].

%%--------------------------------------------------------------------
%% @spec TestCase(Config0) ->
%%               ok | exit() | {skip,Reason} | {comment,Comment} |
%%               {save_config,Config1} | {skip_and_save,Reason,Config1}
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%% Comment = term()
%% @end
%%--------------------------------------------------------------------
insert_query(doc) ->
    ["Test results from insertion with raw SQL."];
insert_query(Config) ->
    {updated, 1} = mysqlerl:sql_query(?config(db_ref, Config),
                                      "INSERT INTO user (username, password) VALUES ('sophia', MD5('tester'))").

insert_param_query(doc) ->
    ["Test results from insertion with parameterized SQL."];
insert_param_query(Config) ->
    {updated, 1} = mysqlerl:param_query(?config(db_ref, Config),
                                        "INSERT INTO user (username, password) VALUES (?, MD5(?))",
                                        [{{sql_varchar, 20}, "satchmo"},
                                         {{sql_varchar, 64}, "thebiggestone"}]).

update_query(doc) ->
    ["Test results from update with raw SQL."];
update_query(Config) ->
    {updated, 1} = mysqlerl:sql_query(?config(db_ref, Config),
                                      "UPDATE user SET password=MD5('newpass') WHERE username='bjc'").

update_param_query(doc) ->
    ["Test results from update with parameterized SQL."];
update_param_query(Config) ->
    {updated, 1} = mysqlerl:param_query(?config(db_ref, Config),
                                        "UPDATE user SET password=MD5(?) WHERE username=?",
                                        [{{sql_varchar, 20}, "siobain"},
                                         {{sql_varchar, 64}, "sosecret"}]).

delete_query(doc) ->
    ["Test results from delete with raw SQL."];
delete_query(Config) ->
    {updated, 1} = mysqlerl:sql_query(?config(db_ref, Config),
                                      "DELETE FROM user WHERE username='bjc'").

delete_param_query(doc) ->
    ["Test results from delete with parameterized SQL."];
delete_param_query(Config) ->
    {updated, 1} = mysqlerl:param_query(?config(db_ref, Config),
                                        "DELETE FROM user WHERE username=?",
                                        [{{sql_varchar, 20}, "siobain"}]).

