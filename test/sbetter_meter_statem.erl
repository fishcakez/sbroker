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

-record(state, {ask_upper :: non_neg_integer(),
                bid_upper :: non_neg_integer(),
                update :: pos_integer(),
                updated :: undefined | integer()}).

module() ->
    sbetter_meter.

args() ->
    {choose(0, 5), choose(0, 5), choose(1, 5)}.

init(Time, {Ask, Bid, Update}) ->
    NAsk = sbroker_util:sojourn_target(Ask),
    NBid = sbroker_util:sojourn_target(Bid),
    NUpdate = sbroker_util:interval(Update),
    {#state{ask_upper=NAsk, bid_upper=NBid, update=NUpdate}, Time}.

update_next(#state{update=NUpdate} = State, Time, _, _, _, _) ->
    {State#state{updated=Time}, NUpdate+Time}.

update_post(#state{ask_upper=Ask, bid_upper=Bid, update=Update}, Time,
            _, QueueDelay, ProcessDelay, RelativeTime) ->
    {lookup_post(QueueDelay, ProcessDelay, RelativeTime, ask, Ask) andalso
     lookup_post(QueueDelay, ProcessDelay, -RelativeTime, ask_r, Bid),
     Update+Time}.

lookup_post(QueueDelay, ProcessDelay, RelativeTime, Queue, Upper) ->
    ObsValue = sbetter_server:lookup(self(), Queue),
    case min(Upper, QueueDelay + ProcessDelay + max(RelativeTime, 0)) of
        ObsValue ->
            true;
        ExpValue ->
            ct:pal("~p value~nExpected: ~p~nObserved: ~p",
                   [Queue, ExpValue, ObsValue]),
            false
    end.

change(_, Time, Args) ->
    init(Time, Args).

timeout(#state{updated=undefined}, Time) ->
    Time;
timeout(#state{update=Update, updated=Updated}, Time) ->
    max(Time, Updated+Update).
