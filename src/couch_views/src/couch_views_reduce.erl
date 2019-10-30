% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

-module(couch_views_reduce).


-export([
    run_reduce/2,
    read_reduce/6,
    setup_reduce_indexes/3
]).


-include("couch_views.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("couch_mrview/include/couch_mrview.hrl").
-include_lib("fabric/include/fabric2.hrl").


-define(LEVEL_FAN_POW, 1).
-define(MAX_SKIP_LIST_LEVELS, 6).


read_reduce(Db, Sig, ViewId, UserCallback, UserAcc0, Args) ->
    #{
        db_prefix := DbPrefix
    } = Db,

%%    Levels = lists:seq(0, ?MAX_SKIP_LIST_LEVELS),
    ReduceIdxPrefix = reduce_skip_list_idx_prefix(DbPrefix, Sig, ViewId),
    #mrargs{
        limit = Limit
    } = Args,

    Opts = args_to_fdb_opts(Args, ReduceIdxPrefix),

    try
        fabric2_fdb:transactional(Db, fun(TxDb) ->
    %%        Levels = lists:seq(0, ?MAX_SKIP_LIST_LEVELS),

            Acc0 = #{
                sig => Sig,
                view_id => ViewId,
                user_acc => UserAcc0,
                args => Args,
                callback => UserCallback,
                reduce_idx_prefix => ReduceIdxPrefix,
                limit => Limit,
                row_count => 0
            },

            Fun = fun handle_row/3,
            Acc1 = couch_views_reduce_fdb:fold_level0(TxDb, Sig, ViewId, Opts, Fun, Acc0),
            #{
                user_acc := UserAcc1
            } = Acc1,
            {ok, maybe_stop(UserCallback(complete, UserAcc1))}
        end)
    catch throw:{done, Out} ->
        {ok, Out}
    end.


args_to_fdb_opts(#mrargs{} = Args, ReduceIdxPrefix) ->
    #mrargs{
%%        limit = Limit,
%%        start_key = StartKey,
%%        end_key = EndKey,
        group = Group,
        group_level = GroupLevel
    } = Args,

    {UStartKey0, EndKey0} = erlfdb_tuple:range({0},
        ReduceIdxPrefix),

    StartKey0 = erlfdb_tuple:pack({0, couch_views_encoding:encode(0, key)}, ReduceIdxPrefix),

%%    StartKey1 = case StartKey of
%%        undefined -> erlfdb_key:first_greater_than(StartKey0);
%%        StartKey -> create_key(StartKey, 0, Red)
%%    end,

    StartKey1 = erlfdb_key:first_greater_than(StartKey0),

    [{streaming_mode, want_all}, {startkey, StartKey1}, {endkey, EndKey0}].


encode_key(Key, Level) ->
    {Level, couch_views_encoding:encode(Key, key)}.


handle_row(Key, Value, Acc) ->
    #{
        callback := UserCallback,
        user_acc := UserAcc0,
        row_count := RowCount,
        limit := Limit
    } = Acc,

    Row = [
        {key, Key},
        {value, Value}
    ],

    RowCountNext = RowCount + 1,

    UserAcc1 = maybe_stop(UserCallback({row, Row}, UserAcc0)),
    Acc1 = Acc#{user_acc := UserAcc1, row_count := RowCountNext},

    case RowCountNext == Limit of
        true ->
            UserAcc2 = maybe_stop(UserCallback(complete, UserAcc1)),
            maybe_stop({stop, UserAcc2});
        false ->
            Acc1
    end.


maybe_stop({ok, Acc}) -> Acc;
maybe_stop({stop, Acc}) -> throw({done, Acc}).

setup_reduce_indexes(Db, Sig, ViewIds) ->
    #{
        db_prefix := DbPrefix
    } = Db,

    fabric2_fdb:transactional(Db, fun(TxDb) ->
        lists:foreach(fun (ViewId) ->
            ViewOpts = #{
                db_prefix => DbPrefix,
                sig => Sig,
                view_id => ViewId
            },
            couch_views_reduce_fdb:create_skip_list(TxDb,
                ?MAX_SKIP_LIST_LEVELS, ViewOpts)
        end, ViewIds)
    end).


run_reduce(#mrst{views = Views } = Mrst, MappedResults) ->
    ReduceFuns = lists:map(fun(View) ->
        #mrview{
            id_num = Id,
            reduce_funs = ViewReduceFuns
        } = View,

        [{_, Fun}] = ViewReduceFuns,
        Fun
    end, Views),

    lists:map(fun (MappedResult) ->
        #{
            results := Results
        } = MappedResult,

        ReduceResults = lists:map(fun ({ReduceFun, Result}) ->
            reduce(ReduceFun, Result)
        end, lists:zip(ReduceFuns, Results)),

        MappedResult#{
            reduce_results => ReduceResults
        }
    end, MappedResults).


reduce(<<"_count">>, Results) ->
    ReduceResults = lists:foldl(fun ({Key, _}, Acc) ->
        case maps:is_key(Key, Acc) of
            true ->
                #{Key := Val} = Acc,
                Acc#{Key := Val + 1};
            false ->
                Acc#{Key => 1}
        end
    end, #{}, Results),
    maps:to_list(ReduceResults);

% this isn't a real supported reduce function in CouchDB
% But I want a basic reduce function that when we need to update the index
% we would need to re-read multiple rows instead of being able to do an
% atomic update
reduce(<<"_stats">>, Results) ->
    ReduceResults = lists:foldl(fun ({Key, Val}, Acc) ->
        io:format("MAX ~p ~p ~n", [Key, Val]),
        case maps:is_key(Key, Acc) of
            true ->
                #{Key := Max} = Acc,
                case Max >= Val of
                    true ->
                        Acc;
                    false ->
                        Acc#{Key := Val}
                end;
            false ->
                Acc#{Key => Val}
        end
    end, #{}, Results),
    maps:to_list(ReduceResults).


is_builtin(<<"_", _/binary>>) ->
    true;

is_builtin(_) ->
    false.

reduce_skip_list_idx_prefix(DbPrefix, Sig, ViewId) ->
    Key = {?DB_VIEWS, Sig, ?VIEW_REDUCE_RANGE, ViewId},
    erlfdb_tuple:pack(Key, DbPrefix).
