%% Modeled from ODBC
%% http://www.erlang.org/doc/apps/odbc/

-module(mysqlerl).
-author('bjc@kublai.com').

-include("mysqlerl.hrl").

-export([convert_type/1]).

-export([test_start/0, test_msg/0, test_query/0, test_param_query/0]).

-export([start/0, start/1, stop/0, commit/2, commit/3,
         connect/6, disconnect/1, describe_table/2,
         describe_table/3, first/1, first/2,
         last/1, last/2, next/1, next/2, prev/1,
         prev/2, select_count/2, select_count/3,
         select/3, select/4, param_query/3, param_query/4,
         sql_query/2, sql_query/3]).

-define(CONFIG, "/Users/bjc/tmp/test-server.cfg").

test_start() ->
    {ok, [{Host, Port, DB, User, Pass, Options}]} = file:consult(?CONFIG),
    mysqlerl:connect(Host, Port, DB, User, Pass, Options).

test_msg() ->
    commit(mysqlerl_connection_sup:random_child(),
           rollback, 2000).

test_query() ->
    sql_query(mysqlerl_connection_sup:random_child(),
              "SELECT COUNT(*) FROM user", 2000).

test_param_query() ->
    %% This should really be an update or something, since that's how
    %% it'll be used.
    param_query(mysqlerl_connection_sup:random_child(),
               "SELECT * FROM user WHERE username=?",
               [{{sql_varchar, 20}, "bjc"}]).

start() ->
    start(temporary).

%% Arguments:
%%     Type = permanent | transient | temporary
%%
%% Returns:
%%     ok | {error, Reason}
start(Type) ->
    application:start(sasl),
    application:start(mysqlerl, Type).

stop() ->
    application:stop(mysqlerl).

commit(Ref, CommitMode) ->
    commit(Ref, CommitMode, infinity).

%% Arguments:
%%     Ref = connection_reference()
%%     Timeout = time_out()
%%     CommitMode = commit | rollback
%%     Reason = not_an_explicit_commit_connection |
%%              process_not_owner_of_odbc_connection |
%%              common_reason()
%%     ok | {error, Reason}
commit(Ref, commit, Timeout) ->
    case sql_query(Ref, "COMMIT", Timeout) of
        {num_rows, _} -> ok;
        Other -> Other
    end;
commit(Ref, rollback, Timeout) ->
    case sql_query(Ref, "ROLLBACK", Timeout) of
        {num_rows, _} -> ok;
        Other -> Other
    end.
    
%% Arguments:
%%     Host = string()
%%     Port = integer()
%%     Database = string()
%%     User = string()
%%     Password = string()
%%     Options = list()
%%
%% Returns:
%%     {ok, Ref} | {error, Reason}
%%     Ref = connection_reference()
connect(Host, Port, Database, User, Password, Options) ->
    mysqlerl_connection_sup:connect(Host, Port, Database,
                                    User, Password, Options).

%% Arguments:
%%     Ref = connection_reference()
%%
%% Returns:
%%     ok | {error, Reason}
disconnect(Ref) ->
    mysqlerl_connection:stop(Ref).

describe_table(Ref, Table) ->
    describe_table(Ref, Table, infinity).

%% Arguments:
%%     Ref = connection_reference()
%%     Table = string()
%%     Timeout = time_out()
%%
%% Returns:
%%     {ok, Description} | {error, Reason}
%%     Description = [{col_name(), odbc_data_type()}]
describe_table(Ref, Table, Timeout) ->
    Q = ["DESCRIBE ", Table],
    {selected, _, Rows} = sql_query(Ref, Q, Timeout),
    Description = [{Name, convert_type(T)} || {Name, T, _, _, _, _} <- Rows],
    {ok, Description}.

first(Ref) ->
    first(Ref, infinity).

%% Arguments:
%%     Ref = connection_reference()
%%     Timeout = time_out()
%% Returns:
%%     {selected, ColNames, Rows} | {error, Reason}
%%     Rows = rows()
first(Ref, Timeout) ->
    conn_fwd(Ref, #sql_first{}, Timeout).

last(Ref) ->
    last(Ref, infinity).

%% Arguments:
%%     Ref = connection_reference()
%%     Timeout = time_out()
%% Returns:
%%     {selected, ColNames, Rows} | {error, Reason}
%%     Rows = rows()
last(Ref, Timeout) ->
    conn_fwd(Ref, #sql_last{}, Timeout).

next(Ref) ->
    next(Ref, infinity).

%% Arguments:
%%     Ref = connection_reference()
%%     Timeout = time_out()
%% Returns:
%%     {selected, ColNames, Rows} | {error, Reason}
%%     Rows = rows()
next(Ref, Timeout) ->
    conn_fwd(Ref, #sql_next{}, Timeout).

prev(Ref) ->
    prev(Ref, infinity).

%% Arguments:
%%     Ref = connection_reference()
%%     Timeout = time_out()
%% Returns:
%%     {selected, ColNames, Rows} | {error, Reason}
%%     Rows = rows()
prev(Ref, Timeout) ->
    conn_fwd(Ref, #sql_prev{}, Timeout).

select_count(Ref, SQLQuery) ->
    select_count(Ref, SQLQuery, infinity).

%% Arguments:
%%     Ref = connection_reference()
%%     SQLQuery = string()
%%     Timeout = time_out()
%% Returns:
%%     {ok, NrRows} | {error, Reason}
%%     NrRows = n_rows()
select_count(Ref, SQLQuery, Timeout) ->
    conn_fwd(Ref, #sql_select_count{q = SQLQuery}, Timeout).

select(Ref, Pos, N) ->
    select(Ref, Pos, N, infinity).

%% Arguments:
%%     Ref = connection_reference()
%%     Pos = integer()
%%     Timeout = time_out()
%% Returns:
%%     {selected, ColNames, Rows} | {error, Reason}
%%     Rows = rows()
select(Ref, Pos, N, Timeout) ->
    conn_fwd(Ref, #sql_select{pos = Pos, n = N}, Timeout).

param_query(Ref, SQLQuery, Params) ->
    param_query(Ref, SQLQuery, Params, infinity).

%% Arguments:
%%     Ref = connection_reference()
%%     SQLQuery = string()
%%     Params = [{odbc_data_type(), [value()]}]
%%     Timeout = time_out()
%% Returns:
%%     {selected, ColNames, Rows} | {error, Reason}
%%     Rows = rows()
param_query(Ref, SQLQuery, Params, Timeout) ->
    conn_fwd(Ref, #sql_param_query{q = SQLQuery, params = Params}, Timeout).

sql_query(Ref, SQLQuery) ->
    sql_query(Ref, SQLQuery, infinity).

%% Arguments:
%%     Ref = connection_reference()
%%     SQLQuery = string()
%%     Timeout = time_out()
%% Returns:
%%     {selected, ColNames, Rows} | {error, Reason}
%%     Rows = rows()
sql_query(Ref, SQLQuery, Timeout) ->
    conn_fwd(Ref, #sql_query{q = SQLQuery}, Timeout).

conn_fwd(Ref, Msg, Timeout) ->
    gen_server:call(Ref, {Msg, Timeout}, infinity).

%% Convert type needs some love! Cover all bases here instead of
%% fudging.
convert_type("timestamp") ->
    sql_timestamp;
convert_type("int") ->
    sql_integer;
convert_type("int(" ++ Rest) ->
    Size = find_data_size(Rest),
    {sql_numeric, list_to_integer(Size)};
convert_type("decimal(" ++ Rest) ->
    Size = find_data_size(Rest),
    {sql_decimal, list_to_integer(Size)};
convert_type("float(" ++ Rest) ->
    Size = find_data_size(Rest),
    {sql_float, list_to_float(Size)};
convert_type("char(" ++ Rest) ->
    Size = find_data_size(Rest),
    {sql_char, list_to_integer(Size)};
convert_type("varchar(" ++ Rest) ->
    Size = find_data_size(Rest),
    {sql_varchar, list_to_integer(Size)}.

find_data_size(Str) ->
    find_data_size(Str, []).

find_data_size([$) | _Rest], Accum) ->
    lists:reverse(Accum);
find_data_size([H | T], Accum) ->
    find_data_size(T, [H | Accum]).
