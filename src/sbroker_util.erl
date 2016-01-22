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
%% @private
-module(sbroker_util).

-export([whereis/1]).
-export([timeout/1]).
-export([sojourn_target/1]).
-export([relative_target/1]).
-export([interval/1]).
-export([min_max/2]).

-type process() :: pid() | atom() | {atom(), node()} | {global, any()} |
    {via, module(), any()}.

-spec whereis(Process) -> Pid | {Name, Node} | undefined when
      Process :: process(),
      Pid :: pid(),
      Name :: atom(),
      Node :: node().
whereis(Pid) when is_pid(Pid) ->
    Pid;
whereis(Name) when is_atom(Name) ->
    erlang:whereis(Name);
whereis({Name, Node}) when is_atom(Name) andalso Node =:= node() ->
    erlang:whereis(Name);
whereis({Name, Node} = Process) when is_atom(Name) andalso is_atom(Node) ->
    Process;
whereis({global, Name}) ->
    global:whereis_name(Name);
whereis({via, Mod, Name}) ->
    Mod:whereis_name(Name).

-spec timeout(Timeout) -> NTimeout when
      Timeout :: timeout(),
      NTimeout :: timeout().
timeout(infinity) ->
    infinity;
timeout(Timeout) ->
    sojourn_target(Timeout).

-spec sojourn_target(Target) -> NTarget when
      Target :: non_neg_integer(),
      NTarget :: non_neg_integer().
sojourn_target(Target) when Target >= 0 ->
    native(Target);
sojourn_target(Other) ->
    error(badarg, [Other]).

-spec relative_target(Target) -> NTarget when
      Target :: integer(),
      NTarget :: integer().
relative_target(Target) ->
      native(Target).

-spec interval(Interval) -> NInterval when
      Interval :: pos_integer(),
      NInterval :: pos_integer().
interval(Interval) when Interval > 0->
    native(Interval);
interval(Other) ->
    error(badarg, [Other]).

-spec min_max(Min, Max) -> {Min, Max} when
      Min :: non_neg_integer(),
      Max :: non_neg_integer() | infinity.
min_max(Min, infinity) when is_integer(Min), Min >= 0 ->
    {Min, infinity};
min_max(Min, Max) when is_integer(Min), is_integer(Max), Min >= 0, Max >= Min ->
    {Min, Max};
min_max(Min, Max) ->
    error(badarg, [Min, Max]).

%% Internal

native(Time) ->
    sbroker_time:convert_time_unit(Time, milli_seconds, native).
