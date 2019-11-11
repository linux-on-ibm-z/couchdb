-module(couch_expiring_cache).

-export([
    lookup/2,
    insert/5
]).


insert(Name, Key, Value, StaleTS, ExpiresTS) ->
    couch_expiring_cache_fdb:insert(Name, Key, Value, StaleTS, ExpiresTS).

lookup(Name, Key) ->
    couch_expiring_cache_fdb:lookup(Name, Key).
