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
-module(sbroker_fair_statem).

-include_lib("proper/include/proper.hrl").

-export([module/0]).
-export([args/0]).
-export([time_dependence/1]).
-export([init/1]).
-export([handle_timeout/3]).
-export([handle_out/3]).
-export([handle_out_r/3]).
-export([config_change/3]).

-record(state, {queue, statem, state, index}).

module() ->
    sbroker_fair_queue.

args() ->
    oneof([{sbroker_drop_queue, sbroker_drop_statem:args(), index()},
           {sbroker_timeout_queue, sbroker_timeout_statem:args(), index()},
           {sbroker_codel_queue, sbroker_codel_statem:args(), index()}]).

index() ->
    frequency([{5, node},
               {1, {hash, node, choose(1, 3)}},
               {1, {hash, oneof([application, pid, value, {element, 1}]), 1}}]).

time_dependence(#state{statem=SMod, state=State}) ->
    SMod:time_dependence(State).

init({QMod, Args, Index}) ->
    SMod = queue2statem(QMod),
    {Out, Drop, Min, Max, State} = SMod:init(Args),
    {Out, Drop, Min, Max,
     #state{queue=QMod, statem=SMod, state=State, index=Index}}.

handle_timeout(Time, L, #state{statem=SMod, state=State} = FullState) ->
    {Drops, NState} = SMod:handle_timeout(Time, L,  State),
    {Drops, FullState#state{state=NState}}.

handle_out(Time, L, #state{statem=SMod, state=State} = FullState) ->
    {Drops, NState} = SMod:handle_out(Time, L, State),
    {Drops, FullState#state{state=NState}}.

handle_out_r(Time, L, #state{statem=SMod, state=State} = FullState) ->
    {Drops, NState} = SMod:handle_out_r(Time, L, State),
    {Drops, FullState#state{state=NState}}.

config_change(Time, {QMod, Args, Index},
              #state{index=Index, queue=QMod, statem=SMod,
                     state=State} = FullState) ->
    {Out, Drop, Min, Max, NState} = SMod:config_change(Time, Args, State),
    {Out, Drop, Min, Max, FullState#state{state=NState}};
config_change(_, FullArgs, _) ->
    init(FullArgs).

%% Internal.

queue2statem(sbroker_drop_queue) ->
    sbroker_drop_statem;
queue2statem(sbroker_timeout_queue) ->
    sbroker_timeout_statem;
queue2statem(sbroker_codel_queue) ->
    sbroker_codel_statem.
