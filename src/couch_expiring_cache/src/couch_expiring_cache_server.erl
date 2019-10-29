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

-module(couch_expiring_cache_server).

-behaviour(gen_server).

-export([
    start_link/0
]).

-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3
]).


-define(DEFAULT_BATCH, 1000).
-define(DEFAULT_PERIOD_MS, 5000).
-define(DEFAULT_MAX_JITTER_MS, 1000).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


init(_) ->
    Ref = schedule_remove_expired(),
    {ok, #{
        timer_ref => Ref,
        lag => 0,
        last_expiration => 0,
        min_ts => 0}}.


terminate(_, _) ->
    ok.


handle_call(Msg, _From, St) ->
    {stop, {bad_call, Msg}, {bad_call, Msg}, St}.


handle_cast(Msg, St) ->
    {stop, {bad_cast, Msg}, St}.


handle_info(remove_expired, St) ->
    Now = erlang:system_time(second),
    MinTS = remove_expired(Now),
    Ref = schedule_remove_expired(),
    {noreply, St#{
        timer_ref => Ref,
        lag => Now - MinTS,
        last_expiration => Now,
        min_ts => MinTS}};

handle_info(Msg, St) ->
    {stop, {bad_info, Msg}, St}.


code_change(_OldVsn, St, _Extra) ->
    {ok, St}.


remove_expired(EndTS) ->
    couch_expiring_cache_fdb:clear_expired_range(EndTS, batch_size()).


schedule_remove_expired() ->
    Timeout = period_ms(),
    MaxJitter = max(Timeout div 2, max_jitter_ms()),
    Wait = Timeout + rand:uniform(max(1, MaxJitter)),
    erlang:send_after(Wait, self(), remove_expired).


period_ms() ->
    config:get_integer("couch_expiring_cache", "period_ms",
        ?DEFAULT_PERIOD_MS).


max_jitter_ms() ->
    config:get_integer("couch_expiring_cache", "max_jitter_ms",
        ?DEFAULT_MAX_JITTER_MS).


batch_size() ->
    config:get_integer("couch_expiring_cache", "batch_size",
        ?DEFAULT_BATCH).
