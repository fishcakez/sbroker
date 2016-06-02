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
%% @doc Registers the process with and updates the `sbetter_server' with
%% approximate queue sojourn times for use with the `sbetter' load balancer.
%%
%% `sbetter_meter' can be used as the `sbroker_meter' in a `sbroker' or
%% a `sregulator'. Its argument is of the form:
%% ```
%% {AskUpper :: non_neg_integer(), AskRUpper :: non_neg_integer(),
%%  Update :: pos_integer()}.
%% ```
%% `AskUpper' is the maximum `ask' sojourn time and `AskRUpper' is the maximum
%% `ask_r' sojourn time that will be updated to the `sbetter_server' for use
%% with `sbetter'. If a match doesn't occur on the `sbroker' or `sregulator' the
%% approximate sojourn time will increase unbounded for one of the two queues.
%% Limiting this value prevents the situation where one process becomes stuck as
%% the "worst" option because it hasn't matched for the longest when the
%% processes' queues would be equivalently "bad".
%%
%% For example if using the `sbroker_timeout_queue' with timeout time of `5000',
%% then all requests are dropped after `5000' and so become approximately
%% equivalent once sojourn time is `5000'.
%%
%% `Update' is the update interval in `milli_seconds'  when the process is idle.
%%
%% @see sbetter
%% @see sbetter_server
-module(sbetter_meter).

-behaviour(sbroker_meter).

-export([init/2]).
-export([handle_update/5]).
-export([handle_info/3]).
-export([code_change/4]).
-export([config_change/3]).
-export([terminate/2]).

%% types

-record(state, {ask :: non_neg_integer(),
                bid :: non_neg_integer(),
                update :: pos_integer(),
                update_next :: integer()}).

%% @private
-spec init(Time, {AskUpper, AskRUpper, Update}) -> {State, Time} when
      Time :: integer(),
      AskUpper :: non_neg_integer(),
      AskRUpper :: non_neg_integer(),
      Update :: pos_integer(),
      State :: #state{}.
init(Time, {Ask, Bid, Update}) ->
    NAsk = sbroker_util:sojourn_target(Ask),
    NBid = sbroker_util:sojourn_target(Bid),
    NUpdate = sbroker_util:interval(Update),
    true = sbetter_server:register(self(), NAsk, NBid),
    {#state{ask=NAsk, bid=NBid, update=NUpdate, update_next=Time}, Time}.

%% @private
-spec handle_update(QueueDelay, ProcessDelay, RelativeTime, Time, State) ->
    {NState, UpdateNext} when
      QueueDelay :: non_neg_integer(),
      ProcessDelay :: non_neg_integer(),
      RelativeTime :: integer(),
      Time :: integer(),
      State :: #state{},
      NState :: #state{},
      UpdateNext :: integer().
handle_update(QueueDelay, ProcessDelay, RelativeTime, Time,
              #state{ask=Ask, bid=Bid, update=Update} = State) ->
    AskSojourn = sojourn(QueueDelay + ProcessDelay, RelativeTime, Ask),
    BidSojourn = sojourn(QueueDelay + ProcessDelay, -RelativeTime, Bid),
    true = sbetter_server:update(self(), AskSojourn, BidSojourn),
    Next = Time + Update,
    {State#state{update_next=Next}, Next}.

%% @private
-spec handle_info(Msg, Time, State) -> {State, UpdateNext} when
      Msg :: any(),
      Time :: integer(),
      State :: #state{},
      UpdateNext :: integer().
handle_info(_, Time, State) ->
    handle(Time, State).

%% @private
-spec code_change(OldVsn, Time, State, Extra) -> {State, UpdateNext} when
      OldVsn :: any(),
      Time :: integer(),
      State :: #state{},
      Extra :: any(),
      UpdateNext :: integer().
code_change(_, Time, State, _) ->
    handle(Time, State).

%% @private
-spec config_change({AskUpper, AskRUpper, Update}, Time, State) ->
    {NState, UpdateNext} when
      AskUpper :: non_neg_integer(),
      AskRUpper :: non_neg_integer(),
      Update :: pos_integer(),
      Time :: integer(),
      State :: #state{},
      NState :: #state{},
      UpdateNext :: integer().
config_change({Ask, Bid, Update}, Time, _) ->
    NAsk = sbroker_util:sojourn_target(Ask),
    NBid = sbroker_util:sojourn_target(Bid),
    NUpdate = sbroker_util:interval(Update),
    {#state{ask=NAsk, bid=NBid, update=NUpdate, update_next=Time}, Time}.

%% @private
-spec terminate(Reason, State) -> true when
      Reason :: any(),
      State :: #state{}.
terminate(_, _) ->
    sbetter_server:unregister(self()).

%% Internal

sojourn(QueueDelay, RelativeTime, Upper) ->
    min(Upper, QueueDelay + max(RelativeTime, 0)).

handle(Time, #state{update_next=Next} = State) ->
    {State, max(Next, Time)}.
