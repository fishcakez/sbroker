%%-------------------------------------------------------------------
%%
%% Copyright (c) 2015, James Fish <james@fishcakez.com>
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License. You may obtain
%% a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied. See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%%-------------------------------------------------------------------
-module(sbroker_test_handler).

-behaviour(gen_event).

%% public api

-export([add_handler/0]).
-export([get_alarms/0]).
-export([reset/0]).
-export([delete_handler/0]).

%% gen_event api
-export([init/1]).
-export([handle_event/2]).
-export([handle_call/2]).
-export([handle_info/2]).
-export([code_change/3]).
-export([terminate/2]).

%% public api

add_handler() ->
    swap_handler(alarm_handler, ?MODULE).

get_alarms() ->
    gen_event:call(alarm_handler, ?MODULE, get_alarms).

reset() ->
    gen_event:call(alarm_handler, ?MODULE, reset).

delete_handler() ->
    swap_handler(?MODULE, alarm_handler).

%% gen_event api

init(_) ->
    {ok, []}.

handle_event({set_alarm, Alarm}, Alarms) ->
    {ok, [Alarm | Alarms]};
handle_event({clear_alarm, Name}, Alarms) ->
    {ok, lists:keydelete(Name, 1, Alarms)}.

handle_call(get_alarms, Alarms) ->
    {ok, Alarms, Alarms};
handle_call(reset, Alarms) ->
    {ok, Alarms, []}.

handle_info(_Info, State) ->
    {ok, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(swap, Alarms) ->
    {?MODULE, Alarms};
terminate(_Reason, _State) ->
    ok.

%% Internal

swap_handler(From, To) ->
    ok = gen_event:add_handler(alarm_handler, To, []),
    gen_event:delete_handler(alarm_handler, From, swap).
