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
-export([timeout/2]).
-export([target/2]).
-export([interval/2]).

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

-spec timeout(Timeout, TimeUnit) -> NTimeout when
      Timeout :: timeout(),
      TimeUnit :: sbroker_time:unit(),
      NTimeout :: timeout().
timeout(infinity, _) ->
    infinity;
timeout(Timeout, TimeUnit) ->
    target(Timeout, TimeUnit).

-spec target(Target, TimeUnit) -> NTarget when
      Target :: integer(),
      TimeUnit :: sbroker_time:unit(),
      NTarget :: integer().
target(Target, TimeUnit) when Target >= 0 ->
    sbroker_time:convert_time_unit(Target, milli_seconds, TimeUnit).

-spec interval(Interval, TimeUnit) -> NInterval when
      Interval :: pos_integer(),
      TimeUnit :: sbroker_time:unit(),
      NInterval :: pos_integer().
interval(Interval, TimeUnit) ->
    case sbroker_time:convert_time_unit(Interval, milli_seconds, TimeUnit) of
        NInterval when NInterval > 0 ->
            NInterval;
        _ ->
            error(badarg, [Interval, TimeUnit])
    end.
