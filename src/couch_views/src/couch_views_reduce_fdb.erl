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


-module(couch_views_reduce_fdb).


-export([
    fold_level0/8,
    create_skip_list/3,
    update_reduce_idx/6
]).


-include("couch_views.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("couch_mrview/include/couch_mrview.hrl").
-include_lib("fabric/include/fabric2.hrl").

-define(MAX_SKIP_LIST_LEVELS, 1).
-define(LEVEL_FAN_POW, 1).

log_levels(Db, Sig, ViewId) ->
    #{
        db_prefix := DbPrefix
    } = Db,

    Levels = lists:seq(0, ?MAX_SKIP_LIST_LEVELS),
    ReduceIdxPrefix = reduce_skip_list_idx_prefix(DbPrefix, Sig, ViewId),
    Opts = [{streaming_mode, want_all}],

    fabric2_fdb:transactional(Db, fun(#{tx := Tx} = TxDb) ->
        lists:foldl(fun (Level, Level0Total) ->
            {StartKey, EndKey} = erlfdb_tuple:range({Level},
                ReduceIdxPrefix),

            Future = erlfdb:get_range(Tx, StartKey, EndKey, Opts),
            Rows = lists:map(fun ({_Key, EV}) ->
                unpack_key_value(EV)
            end, erlfdb:wait(Future)),

            io:format("~n LEVEL ~p rows ~p ~n", [Level, Rows]),
            case Level == 0 of
                true ->
                    sum_rows(Rows);
                false ->
                    Total = sum_rows(Rows),
                    if Total == Level0Total -> Level0Total; true ->
                        io:format("~n ~n LEVEL ~p NOT EQUAL ~p ~p ~n", [Level, Level0Total, Total])
%%                        throw(level_total_error)
                    end
            end

        end, 0, Levels)
    end).

sum_rows(Rows) ->
    lists:foldl(fun ({_, Val}, Sum) ->
        Val + Sum
    end, 0, Rows).


%%fold(Db, Sig, ViewId, Options, Callback, Acc0) ->
%%    #{
%%        db_prefix := DbPrefix
%%    } = Db,
%%
%%%%    Levels = lists:seq(0, ?MAX_SKIP_LIST_LEVELS),
%%    ReduceIdxPrefix = reduce_skip_list_idx_prefix(DbPrefix, Sig, ViewId),
%%    #mrargs{
%%        limit = Limit
%%    } = Args,
%%
%%    fabric2_fdb:transactional(Db, fun(TxDb) ->
%%
%%        Acc0 = #{
%%            sig => Sig,
%%            view_id => ViewId,
%%            user_acc => UserAcc0,
%%            args => Args,
%%            callback => UserCallback,
%%            reduce_idx_prefix => ReduceIdxPrefix,
%%            limit => Limit,
%%            row_count => 0
%%
%%        },
%%
%%        Acc1 = read_level0_only(TxDb, Acc0, Callback),
%%        #{
%%            user_acc := UserAcc1
%%        } = Acc1,
%%        {ok, UserAcc1}
%%    end).

fold_level0(Db, Sig, ViewId, Reducer, GroupLevel, Opts, UserCallback, UserAcc0) ->
    #{
        db_prefix := DbPrefix
    } = Db,

    Level = 0,
    ReduceIdxPrefix = reduce_skip_list_idx_prefix(DbPrefix, Sig, ViewId),
    Acc = #{
        sig => Sig,
        view_id => ViewId,
        user_acc => UserAcc0,
        %%            args := Args,
        callback => UserCallback,
        reduce_idx_prefix => ReduceIdxPrefix,
        reducer => Reducer,
        group_level => GroupLevel,
        rows => []
    },

    fabric2_fdb:transactional(Db, fun(TxDb) ->
        log_levels(TxDb, Sig, ViewId),
        #{
            tx := Tx
        } = TxDb,


        {startkey, StartKey} = lists:keyfind(startkey, 1, Opts),
        {endkey, EndKey} = lists:keyfind(endkey, 1, Opts),

        Fun = fun fold_fwd_cb/2,
        Acc1 = erlfdb:fold_range(Tx, StartKey, EndKey, Fun, Acc, Opts),
        #{
            user_acc := UserAcc1,
            rows := Rows
        } = Acc1,

        rereduce_and_reply(Reducer, Rows, GroupLevel,
            UserCallback, UserAcc1)
    end).


fold_fwd_cb({_FullEncodedKey, EV}, Acc) ->
    #{
        callback := Callback,
        user_acc := UserAcc,
        group_level := GroupLevel,
        rows := Rows,
        reducer := Reducer
    } = Acc,

    {Key, Val} = unpack_key_value(EV),

    LastKey = if Rows == [] -> false; true ->
        {LastKey0, _} = lists:last(Rows),
        LastKey0
    end,

    case group_level_equal(Key, LastKey, GroupLevel) of
        true ->
            Acc#{
                rows := Rows ++ [{Key, Val}]
            };
        false ->
            UserAcc1 = rereduce_and_reply(Reducer, Rows, GroupLevel,
                Callback, UserAcc),
            Acc#{
                user_acc := UserAcc1,
                rows := [{Key, Val}]
            }
    end.

rereduce_and_reply(_Reducer, [], _GroupLevel, _Callback, Acc) ->
    Acc;

rereduce_and_reply(Reducer, Rows, GroupLevel, Callback, Acc) ->
    {ReducedKey, ReducedVal} = rereduce(Reducer, Rows, GroupLevel),
    Callback(ReducedKey, ReducedVal, Acc).


rereduce(_Reducer, [], _GroupLevel) ->
    no_kvs;

rereduce(_Reducer, Rows, GroupLevel) when length(Rows) == 1 ->
    {Key, Val} = hd(Rows),
    GroupKey = group_level_key(Key, GroupLevel),
    {GroupKey, Val};

rereduce(<<"_count">>, Rows, GroupLevel) ->
    Val = length(Rows),
    {Key, _} = hd(Rows),
    GroupKey = group_level_key(Key, GroupLevel),
    {GroupKey, Val}.


group_level_equal(_One, _Two, 0) ->
    true;

group_level_equal(_One, _Two, group_true) ->
    false;

group_level_equal(One, Two, GroupLevel) ->
    GroupOne = group_level_key(One, GroupLevel),
    GroupTwo = group_level_key(Two, GroupLevel),
    GroupOne == GroupTwo.


group_level_key(_Key, 0) ->
    null;

group_level_key(Key, GroupLevel) when is_list(Key) ->
    lists:sublist(Key, GroupLevel);

group_level_key(Key, _GroupLevel) ->
    Key.


reduce_skip_list_idx_prefix(DbPrefix, Sig, ViewId) ->
    Key = {?DB_VIEWS, Sig, ?VIEW_REDUCE_RANGE, ViewId},
    erlfdb_tuple:pack(Key, DbPrefix).


unpack_key_value(EncodedValue) ->
    {EK, EV1} = erlfdb_tuple:unpack(EncodedValue),
    Key = couch_views_encoding:decode(EK),
    Val = couch_views_encoding:decode(EV1),
    {Key, Val}.


%% Inserting
update_reduce_idx(TxDb, Sig, ViewId, _DocId, _ExistingKeys, ReduceResult) ->
    #{
        db_prefix := DbPrefix
    } = TxDb,

    ViewOpts = #{
        db_prefix => DbPrefix,
        sig => Sig,
        view_id => ViewId
    },

    lists:foreach(fun ({Key, Val}) ->
        io:format("RESULTS KV ~p ~p ~n", [Key, Val]),
        add_kv_to_skip_list(TxDb, ?MAX_SKIP_LIST_LEVELS, ViewOpts, Key, Val)
    end, ReduceResult).


create_skip_list(Db, MaxLevel, #{} = ViewOpts) ->
    #{
        db_prefix := DbPrefix,
        sig := Sig,
        view_id := ViewId
    } = ViewOpts,

    Levels = lists:seq(0, MaxLevel),
    ReduceIdxPrefix = reduce_skip_list_idx_prefix(DbPrefix, Sig, ViewId),

    fabric2_fdb:transactional(Db, fun(TxDb) ->

        lists:foreach(fun(Level) ->
            add_kv(TxDb, ReduceIdxPrefix, Level, 0, 0)
        end, Levels)
    end).


add_kv_to_skip_list(Db, MaxLevel, #{} = ViewOpts, Key, Val) ->
    #{
        db_prefix := DbPrefix,
        sig := Sig,
        view_id := ViewId
    } = ViewOpts,

    Levels = lists:seq(0, MaxLevel),
    ReduceIdxPrefix = reduce_skip_list_idx_prefix(DbPrefix, Sig, ViewId),
    KeyHash = hash_key(Key),

    fabric2_fdb:transactional(Db, fun(TxDb) ->
        lists:foreach(fun(Level) ->
            {PrevKey, PrevVal} = get_previous_key(TxDb, ReduceIdxPrefix, Level, Key),
            io:format("Level ~p K/V ~p ~p PREV KV ~p ~p ~n", [Level, Key, Val, PrevKey, PrevVal]),
            case should_add_key_to_level(Level, KeyHash) of
                true ->
                    io:format("Adding at ~p ~p ~n", [Level, Key]),
                    add_kv(Db, ReduceIdxPrefix, Level, Key, Val);
                false ->
%%                    {PrevKey, NewVal} = rereduce(<<"_stats">>, {PrevKey, PrevVal}, {Key, Val}),
%%                    io:format("RE_REDUCE ~p ~p ~p ~p ~n", [Level, Key, PrevKey, NewVal]),
                    add_kv(Db, ReduceIdxPrefix, Level, PrevKey, PrevVal)
            end
        end, Levels)
    end).


get_previous_key(TxDb, ReduceIdxPrefix, Level, Key) ->
    #{
        tx := Tx
    } = TxDb,

    % TODO: see if we need to add in conflict ranges for this for level=0
    Opts = [{limit, 1}, {streaming_mode, want_all}],

    EK = couch_views_encoding:encode(Key, key),
    StartKey = erlfdb_tuple:pack({Level, EK}, ReduceIdxPrefix),
    StartKeySel = erlfdb_key:last_less_than(StartKey),
    EndKeySel = erlfdb_key:first_greater_or_equal(StartKey),

    Future = erlfdb:get_range(Tx, StartKeySel, EndKeySel, Opts),
    [{_FullEncodedKey, PackedValue}] = erlfdb:wait(Future),
    get_key_value(PackedValue).


hash_key(Key) ->
    erlang:phash2(Key).


should_add_key_to_level(0, _KeyHash) ->
    true;

should_add_key_to_level(Level, KeyHash) ->
    (KeyHash band ((1 bsl (Level * ?LEVEL_FAN_POW)) -1)) == 0.
%%    keyHash & ((1 << (level * LEVEL_FAN_POW)) - 1)) != 0


create_key(ReduceIdxPrefix, SkipLevel, Key) ->
    EK = couch_views_encoding:encode(Key, key),
    LevelKey = {SkipLevel, EK},
    erlfdb_tuple:pack(LevelKey, ReduceIdxPrefix).


create_value(Key, Val) ->
    EK = couch_views_encoding:encode(Key),
    EV = couch_views_encoding:encode(Val),
    erlfdb_tuple:pack({EK, EV}).


get_key_value(PackedValue) ->
    {EncodedKey, EncodedValue}
        = erlfdb_tuple:unpack(PackedValue),
    Key = couch_views_encoding:decode(EncodedKey),
    Value = couch_views_encoding:decode(EncodedValue),
    {Key, Value}.


add_kv(TxDb, ReduceIdxPrefix, Level, Key, Val) ->
    #{
        tx := Tx
    } = TxDb,

    LevelKey = create_key(ReduceIdxPrefix, Level, Key),
    EV = create_value(Key, Val),

    ok = erlfdb:set(Tx, LevelKey, EV).


%%rereduce(<<"_stats">>, {PrevKey, PrevVal}, {_Key, Val}) ->
%%    case PrevVal >= Val of
%%        true -> {PrevKey, PrevVal};
%%        false -> {PrevKey, Val}
%%    end.



