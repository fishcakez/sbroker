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
-module(sbroker_meter).

-behaviour(sbroker_handlers).

%% sbroker_handlers api

-export([initial_state/0]).
-export([init/4]).
-export([config_change/4]).
-export([terminate/3]).

%% types

-callback init(Time :: integer(), Args :: any()) -> State :: any().

-callback handle_update(QueueDelay :: non_neg_integer(),
                        ProcessDelay :: non_neg_integer(), Time :: integer(),
                        State :: any()) ->
    {NState :: any(), UpdateTime :: integer() | infinity}.

-callback handle_info(Msg :: any(), Time :: integer(), State :: any()) ->
    {NState :: any(), UpdateTime :: integer() | infinity}.

-callback config_change(Args :: any(), Time :: integer(), State :: any()) ->
    {NState :: any(), UpdateTime :: integer() | infinity}.

-callback terminate(Reason :: sbroker_handler:reason(), State :: any()) ->
    any().

%% sbroker_handlers api

%% @private
-spec initial_state() -> undefined.
initial_state() ->
    undefined.

%% @private
-spec init(Module, any(), Time, Args) -> {State, TimeoutTime} when
    Module :: module(),
    Time :: integer(),
    Args :: any(),
    State :: any(),
    TimeoutTime :: integer() | infinity.
init(Mod, _, Now, Args) ->
    Mod:init(Now, Args).

%% @private
-spec config_change(Module, Args, Time, State) ->
    {NState, TimeoutTime} when
    Module :: module(),
    Args :: any(),
    Time :: integer(),
    State :: any(),
    NState :: any(),
    TimeoutTime :: integer() | infinity.
config_change(Mod, Args, Now, State) ->
    Mod:config_change(Args, Now, State).

%% @private
-spec terminate(Module, Reason, State) -> undefined when
      Module :: module(),
      Reason :: sbroker_handlers:reason(),
      State :: any().
terminate(Mod, Reason, State) ->
    _ = Mod:terminate(Reason, State),
    undefined.
