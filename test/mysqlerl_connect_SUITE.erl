%%%-------------------------------------------------------------------
%%% @author Brian Cully <bjc@kublai.com>
%%% @copyright (C) 2012, Brian Cully
%%% @doc
%%%
%%% @end
%%% Created :  9 Feb 2012 by Brian Cully <bjc@kublai.com>
%%%-------------------------------------------------------------------
-module(mysqlerl_connect_SUITE).

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
    mysqlerl_test_lib:create_db(Config),
    ok = application:start(mysqlerl),
    Config.

%%--------------------------------------------------------------------
%% @spec end_per_suite(Config0) -> void() | {save_config,Config1}
%% Config0 = Config1 = [tuple()]
%% @end
%%--------------------------------------------------------------------
end_per_suite(Config) ->
    ok = application:stop(mysqlerl),
    mysqlerl_test_lib:drop_db(Config).

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
    Config.

%%--------------------------------------------------------------------
%% @spec end_per_testcase(TestCase, Config0) ->
%%               void() | {save_config,Config1} | {fail,Reason}
%% TestCase = atom()
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%% @end
%%--------------------------------------------------------------------
end_per_testcase(_TestCase, _Config) ->
    ok.

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
    [valid_connect, valid_disconnect, no_port_driver,
     port_dies, owner_dies, controller_dies].

%%--------------------------------------------------------------------
%% @spec TestCase(Config0) ->
%%               ok | exit() | {skip,Reason} | {comment,Comment} |
%%               {save_config,Config1} | {skip_and_save,Reason,Config1}
%% Config0 = Config1 = [tuple()]
%% Reason = term()
%% Comment = term()
%% @end
%%--------------------------------------------------------------------
valid_connect(doc) ->
    ["Test that a normal connection works."];
valid_connect(Config) ->
    DBInfo = ct:get_config(db_info),
    {ok, DBRef} = mysqlerl:connect(?config(host, DBInfo),
                                   ?config(port, DBInfo),
                                   ?config(name, DBInfo),
                                   ?config(username, DBInfo),
                                   ?config(password, DBInfo),
                                   ?config(options, DBInfo)),
    [{db_ref, DBRef} | Config].

valid_disconnect(doc) ->
    ["Test that disconnection works with open connection."];
valid_disconnect(Config) ->
    ok = mysqlerl:disconnect(?config(db_ref, Config)).

no_port_driver(doc) ->
    ["Test that connection fails properly when the port driver is missing."];
no_port_driver(_Config) ->
    Dir = filename:nativename(filename:join(code:priv_dir(mysqlerl), "bin")),
    FN1 = filename:nativename(os:find_executable("mysqlerl", Dir)),
    FN2 = filename:nativename(filename:join(Dir, "mysqlerl.bak")),
    ok = file:rename(FN1, FN2),

    DBInfo = ct:get_config(db_info),
    Res = case catch mysqlerl:connect(?config(host, DBInfo),
                                      ?config(port, DBInfo),
                                      ?config(name, DBInfo),
                                      ?config(username, DBInfo),
                                      ?config(password, DBInfo),
                                      ?config(options, DBInfo)) of
              {error, port_program_executable_not_found} -> ok;
              Other                                      -> Other
          end,

    ok = file:rename(FN2, FN1),
    ok = Res.

port_dies(doc) ->
    ["Test that port driver crashes properly."];
port_dies(_Config) ->
    DBInfo = ct:get_config(db_info),
    {ok, Ref} = mysqlerl:connect(?config(host, DBInfo),
                                 ?config(port, DBInfo),
                                 ?config(name, DBInfo),
                                 ?config(username, DBInfo),
                                 ?config(password, DBInfo),
                                 ?config(options, DBInfo)),
    {status, _} = process_info(Ref, status),
    MonRef = erlang:monitor(process, Ref),
    Port = find_port(Ref),
    exit(Port, kill),

    receive
        {'DOWN', MonRef, _Type, _Object, _Info} ->
            ok
    after 5000 ->
            test_server:fail(owner_process_not_stopped)
    end.

owner_dies(doc) ->
    ["Test that port closes when owner dies."];
owner_dies(_Config) ->
    PID = spawn(?MODULE, owner_fun, [self()]),
    MonRef = receive
                 {ref, Ref} ->
                     MRef = erlang:monitor(process, Ref),
                     PID ! continue,
                     MRef
             end,

    receive
        {'DOWN', MonRef, _Type, _Object, _Info} ->
            ok
    after 5000 ->
            test_server:fail(owner_process_not_stopped)
    end.

owner_fun(PID) ->
    DBInfo = ct:get_config(db_info),
    {ok, Ref} = mysqlerl:connect(?config(host, DBInfo),
                                 ?config(port, DBInfo),
                                 ?config(name, DBInfo),
                                 ?config(username, DBInfo),
                                 ?config(password, DBInfo),
                                 ?config(options, DBInfo)),
    PID ! {ref, Ref},
    receive
        continue -> ok
    end,
    exit(self(), normal).

controller_dies(doc) ->
    ["Test that connection controller death kills the port."];
controller_dies(_Config) ->
    DBInfo = ct:get_config(db_info),
    {ok, Ref} = mysqlerl:connect(?config(host, DBInfo),
                                 ?config(port, DBInfo),
                                 ?config(name, DBInfo),
                                 ?config(username, DBInfo),
                                 ?config(password, DBInfo),
                                 ?config(options, DBInfo)),
    Port = find_port(Ref),
    {connected, Ref} = erlang:port_info(Port, connected),
    exit(Ref, kill),
    test_server:sleep(500),
    undefined = erlang:port_info(Port, connected).

find_port(Ref) ->
    find_port(Ref, erlang:ports()).

find_port(Ref, [Port | T]) ->
    case proplists:lookup(connected, erlang:port_info(Port)) of
        {connected, Ref} -> Port;
        _                -> find_port(Ref, T)
    end;
find_port(_Ref, []) ->
    undefined.
