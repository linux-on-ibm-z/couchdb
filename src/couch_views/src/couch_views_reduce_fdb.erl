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
    fold_level0/6,
    create_skip_list/3,
    update_reduce_idx/6
]).


-include("couch_views.hrl").
-include_lib("couch/include/couch_db.hrl").
-include_lib("couch_mrview/include/couch_mrview.hrl").
-include_lib("fabric/include/fabric2.hrl").

-define(MAX_SKIP_LIST_LEVELS, 6).
-define(LEVEL_FAN_POW, 1).

log_levels(Db, Sig, ViewId) ->
    #{
        db_prefix := DbPrefix
    } = Db,

    Levels = lists:seq(0, 6),
    ReduceIdxPrefix = reduce_skip_list_idx_prefix(DbPrefix, Sig, ViewId),
    Opts = [{streaming_mode, want_all}],

    fabric2_fdb:transactional(Db, fun(#{tx := Tx} = TxDb) ->
        lists:foreach(fun (Level) ->
            {StartKey, EndKey} = erlfdb_tuple:range({Level},
                ReduceIdxPrefix),

            Acc0 = #{
                sig => Sig,
                view_id => ViewId,
                reduce_idx_prefix => ReduceIdxPrefix,
                user_acc => [],
                callback => fun handle_log_levels/3
            },

            Fun = fun fold_fwd_cb/2,
            Acc = erlfdb:fold_range(Tx, StartKey, EndKey, Fun, Acc0, Opts),
            #{
                user_acc := Rows
            } = Acc,
            io:format("~n LEVEL ~p rows ~p ~n", [Level, Rows])
        end, Levels)
    end).

handle_log_levels(Key, Value, Acc) ->
    Acc ++ [{Key, Value}].

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

fold_level0(Db, Sig, ViewId, Opts, UserCallback, UserAcc0) ->
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
        reduce_idx_prefix => ReduceIdxPrefix
    },

    fabric2_fdb:transactional(Db, fun(TxDb) ->
        log_levels(TxDb, Sig, ViewId),
        #{
            tx := Tx
        } = TxDb,


%%        {StartKey, EndKey} = erlfdb_tuple:range({0},
%%            ReduceIdxPrefix),
        {startkey, StartKey} = lists:keyfind(startkey, 1, Opts),
        {endkey, EndKey} = lists:keyfind(endkey, 1, Opts),

%%        ReduceIdxPrefix = reduce_skip_list_idx_prefix(DbPrefix, Sig, ViewId),
        Fun = fun fold_fwd_cb/2,
        Acc1 = erlfdb:fold_range(Tx, StartKey, EndKey, Fun, Acc, Opts),
        #{
            user_acc := UserAcc1
        } = Acc1,
        UserAcc1
    end).


fold_fwd_cb({FullEncodedKey, EV}, Acc) ->
    #{
        reduce_idx_prefix := ReduceIdxPrefix,
        callback := Callback,
        user_acc := UserAcc
    } = Acc,

    {_Level, _EK}
        = erlfdb_tuple:unpack(FullEncodedKey, ReduceIdxPrefix),
    {EK, EV1} = erlfdb_tuple:unpack(EV),
    Key = couch_views_encoding:decode(EK),
    Val = couch_views_encoding:decode(EV1),

    UserAcc1 = Callback(Key, Val, UserAcc),
    Acc#{user_acc := UserAcc1}.


reduce_skip_list_idx_prefix(DbPrefix, Sig, ViewId) ->
    Key = {?DB_VIEWS, Sig, ?VIEW_REDUCE_RANGE, ViewId},
    erlfdb_tuple:pack(Key, DbPrefix).


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
            io:format("Process ~p ~p ~p PREV VALS ~p ~p ~n", [Level, Key, Val, PrevKey, PrevVal]),
            case should_add_key_to_level(Level, KeyHash) of
                true ->
                    io:format("Adding ~p ~p ~n", [Level, Key]),
                    add_kv(Db, ReduceIdxPrefix, Level, Key, Val);
                false ->
                    {PrevKey, NewVal} = rereduce(<<"_stats">>, {PrevKey, PrevVal}, {Key, Val}),
                    io:format("RE_REDUCE ~p ~p ~p ~p ~n", [Level, Key, PrevKey, NewVal]),
                    add_kv(Db, ReduceIdxPrefix, Level, PrevKey, NewVal)
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
    StartKeySel = erlfdb_key:last_less_or_equal(StartKey),
    EndKeySel = erlfdb_key:first_greater_or_equal(StartKey),

    Future = erlfdb:get_range(Tx, StartKeySel, EndKeySel, Opts),
    [{_FullEncodedKey, PackedValue}] = erlfdb:wait(Future),
    get_key_value(PackedValue).


hash_key(Key) ->
    erlang:phash2(Key).


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


rereduce(<<"_stats">>, {PrevKey, PrevVal}, {_Key, Val}) ->
    case PrevVal >= Val of
        true -> {PrevKey, PrevVal};
        false -> {PrevKey, Val}
    end.



