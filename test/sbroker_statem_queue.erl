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
-module(sbroker_statem_queue).

-behaviour(squeue).

-export([init/1]).
-export([handle_timeout/3]).
-export([handle_enqueue/3]).
-export([handle_dequeue/3]).
-export([handle_join/3]).


%% This squeue backend takes a list of non_neg_integer() and drops the integer
%% at head of the list (or the whole queue if the queue length is lower). The
%% tail is kept and used for the next call. Once the list is emptied the
%% original list is used in its place, if this list is empty no drops occcur.
%%
%% Time is ignored completely to allow testing independent of time in sbroker.
%% Timing in `squeue` is tested in squeue_statem where the test controls the
%% time.

init(Drops) ->
    {Drops, Drops}.

handle_timeout(_, Q, State) ->
    handle(Q, State).

handle_enqueue(_, Q, State) ->
    handle(Q, State).

handle_dequeue(_, Q, State) ->
    handle(Q, State).

handle_join(_, Q, State) ->
    {[], Q, State}.

%% If queue is empty don't change state.
handle(Q, State) ->
    case queue:is_empty(Q) of
        true ->
            {[], Q, State};
        false ->
            do_handle(Q, State)
    end.

do_handle(Q, {[], []} = State) ->
    {[], Q, State};
do_handle(Q, {[], Drops}) ->
    do_handle(Q, {Drops, Drops});
do_handle(Q, {[N | Rest], Drops}) ->
    Split = min(N, queue:len(Q)),
    {DropQ, NQ} = queue:split(Split, Q),
    {queue:to_list(DropQ), NQ, {Rest, Drops}}.
