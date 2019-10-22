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


-define(PERIOD_DEFAULT, 10).
-define(MAX_JITTER_DEFAULT, 1).


start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


init(_) ->
    Ref = schedule_remove_expired(),
    {ok, #{timer_ref => Ref}}.


terminate(_, _) ->
    ok.


handle_call(Msg, _From, St) ->
    {stop, {bad_call, Msg}, {bad_call, Msg}, St}.


handle_cast(Msg, St) ->
    {stop, {bad_cast, Msg}, St}.


handle_info(remove_expired, St) ->
    ok = remove_expired(),
    Ref = schedule_remove_expired(),
    {noreply, St#{timer_ref => Ref}};

handle_info(Msg, St) ->
    {stop, {bad_info, Msg}, St}.


code_change(_OldVsn, St, _Extra) ->
    {ok, St}.


remove_expired() ->
    Now = erlang:system_time(second),
    Limit = 10,
    Expired = couch_expiring_cache_fdb:get_range(Now, Limit),
    case Expired of
        [] ->
            ok;
        _ ->
            lists:foreach(fun({TS, Name, Key} = Exp) ->
                couch_log:info("~p remove_expired ~p", [?MODULE, Exp]),
                couch_expiring_cache_fdb:remove_exp(TS, Name, Key)
            end, Expired)
    end.


schedule_remove_expired() ->
    Timeout = get_period_sec(),
    MaxJitter = max(Timeout div 2, get_max_jitter_sec()),
    Wait = Timeout + rand:uniform(max(1, MaxJitter)),
    erlang:send_after(Wait * 1000, self(), remove_expired).


get_period_sec() ->
    config:get_integer("couch_expiring_cache", "period_sec",
        ?PERIOD_DEFAULT).


get_max_jitter_sec() ->
    config:get_integer("couch_expiring_cache", "max_jitter_sec",
        ?MAX_JITTER_DEFAULT).
