%%-------------------------------------------------------------------
%%
%% Copyright (c) 2016, James Fish <james@fishcakez.com>
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
-module(sbetter_meter_statem).

-include_lib("proper/include/proper.hrl").

-export([module/0]).
-export([args/0]).
-export([init/2]).
-export([update_next/6]).
-export([update_post/6]).
-export([change/3]).
-export([timeout/2]).

module() ->
    sbetter_meter.

args() ->
    undefined.

init(Time, undefined) ->
    {undefined, Time}.

update_next(undefined, _, _, _, _, _) ->
    {undefined, infinity}.

update_post(undefined, _, _, QueueDelay, ProcessDelay, RelativeTime) ->
    {lookup_post(QueueDelay, ProcessDelay, RelativeTime, ask) andalso
     lookup_post(QueueDelay, ProcessDelay, -RelativeTime, ask_r), infinity}.

lookup_post(QueueDelay, ProcessDelay, RelativeTime, Queue) ->
    ObsValue = sbetter_server:lookup(self(), Queue),
    case QueueDelay + ProcessDelay + max(RelativeTime, 0) of
        ObsValue ->
            true;
        ExpValue ->
            ct:pal("~p value~nExpected: ~p~nObserved: ~p",
                   [Queue, ExpValue, ObsValue]),
            false
    end.

change(undefined, _, _) ->
    {undefined, infinity}.

timeout(_, _) ->
    infinity.
