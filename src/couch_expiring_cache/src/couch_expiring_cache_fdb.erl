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

-module(couch_expiring_cache_fdb).

-export([
    get_range/2,
    cache_prefix/1,
    layer_prefix/1,
    list_keys/1,
    list_exp/0,
    remove_exp/3,
    %% delete_all/1,
    insert/5,
    lookup/2,
    remove/2
]).


-define(DEFAULT_LIMIT, 100000).

-define(XC, 53). % coordinate with fabric2.hrl
-define(PK, 1).
-define(EXP, 2).


% Data model
% see: https://forums.foundationdb.org/t/designing-key-value-expiration-in-fdb/156
%
% (?XC, ?PK, Name, Key) := (Val, StaleTS, ExpireTS)
% (?XC, ?EXP, ExpireTS, Name, Key) := ()


list_keys(Name) ->
    Callback = fun(Key, Acc) -> [Key | Acc] end,
    fabric2_fdb:transactional(fun(Tx) ->
        list_keys_int(Name, Callback, Tx)
    end).

list_keys_int(Name, Callback, Tx) ->
    Prefix = erlfdb_tuple:pack({?XC, ?PK, Name}, layer_prefix(Tx)),
    fabric2_fdb:fold_range({tx, Tx}, Prefix, fun({K, _V}, Acc) ->
        {Key} = erlfdb_tuple:unpack(K, Prefix),
        Callback(Key, Acc)
    end, [], []).


list_exp() ->
    Callback = fun(Key, Acc) -> [Key | Acc] end,
    fabric2_fdb:transactional(fun(Tx) ->
        Prefix = erlfdb_tuple:pack({?XC, ?EXP}, layer_prefix(Tx)),
        fabric2_fdb:fold_range({tx, Tx}, Prefix, fun({K, _V}, Acc) ->
            Unpacked = {_ExpiresTS, _Name, _Key} = erlfdb_tuple:unpack(K, Prefix),
            Callback(Unpacked, Acc)
        end, [], [])
    end).


get_range(EndTS, Limit) when Limit > 0 ->
    Callback = fun(Key, Acc) -> [Key | Acc] end,
    fabric2_fdb:transactional(fun(Tx) ->
        Prefix = erlfdb_tuple:pack({?XC, ?EXP}, layer_prefix(Tx)),
        fabric2_fdb:fold_range({tx, Tx}, Prefix, fun({K, _V}, Acc) ->
            Unpacked = {_ExpiresTS, _Name, _Key} = erlfdb_tuple:unpack(K, Prefix),
            Callback(Unpacked, Acc)
        end, [], [{end_key, EndTS}, {limit, Limit}])
    end).


remove_exp(ExpiresTS, Name, Key) ->
    fabric2_fdb:transactional(fun(Tx) ->
        Prefix = layer_prefix(Tx),

        PK = erlfdb_tuple:pack({?XC, ?PK, Name, Key}, Prefix),
        XK = erlfdb_tuple:pack({?XC, ?EXP, ExpiresTS, Name, Key}, Prefix),
        ok = erlfdb:clear(Tx, PK),
        ok = erlfdb:clear(Tx, XK)
    end).


insert(Name, Key, Val, StaleTS, ExpiresTS) ->
    fabric2_fdb:transactional(fun(Tx) ->
        Prefix = layer_prefix(Tx),

        PK = erlfdb_tuple:pack({?XC, ?PK, Name, Key}, Prefix),
        PV = erlfdb_tuple:pack({Val, StaleTS, ExpiresTS}),
        ok = erlfdb:set(Tx, PK, PV),

        XK = erlfdb_tuple:pack({?XC, ?EXP, ExpiresTS, Name, Key}, Prefix),
        XV = erlfdb_tuple:pack({}),
        ok = erlfdb:set(Tx, XK, XV)
    end).


lookup(Name, Key) ->
    fabric2_fdb:transactional(fun(Tx) ->
        Prefix = layer_prefix(Tx),

        PK = erlfdb_tuple:pack({?XC, ?PK, Name, Key}, Prefix),
        case erlfdb:wait(erlfdb:get(Tx, PK)) of
            not_found ->
                not_found;
            Bin when is_binary(Bin) ->
                {Val, StaleTS, ExpiresTS} = erlfdb_tuple:unpack(Bin),
                Now = erlang:system_time(second),
                if
                    Now < StaleTS -> {fresh, Val};
                    Now < ExpiresTS -> {stale, Val};
                    true -> expired
                end
        end
    end).


remove(Name, Key) ->
    fabric2_fdb:transactional(fun(Tx) ->
        Prefix = layer_prefix(Tx),

        PK = erlfdb_tuple:pack({?XC, ?PK, Name, Key}, Prefix),
        erlfdb:clear(Tx, PK)
    end).


layer_prefix(Tx) ->
    fabric2_fdb:get_dir(Tx).


cache_prefix(Tx) ->
    erlfdb_tuple:pack({?XC}, layer_prefix(Tx)).



-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.
