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

-export([out/1]).
-export([drop/1]).
-export([timeout/1]).
-export([sojourn_target/1]).
-export([sojourn_target/2]).
-export([relative_target/1]).
-export([interval/1]).
-export([interval/2]).
-export([update/1]).
-export([min_max/1]).
-export([max/1]).
-export([upper/2]).
-export([limit/1]).
-export([alarm/2]).
-export([uniform_interval_s/2]).

-spec out(#{out => Out}) -> Out when
      Out :: out | out_r.
out(#{out := out})      -> out;
out(#{out := out_r})    -> out_r;
out(#{out := _} = Spec) -> error(badarg, [Spec]);
out(#{})                -> out.

-spec drop(#{drop => Drop}) -> Drop when
      Drop :: drop | drop_r.
%% If maximum is zero then tail is always dropped
drop(#{drop := drop, max := 0}) -> drop_r;
drop(#{drop := drop})           -> drop;
drop(#{drop := drop_r})         -> drop_r;
drop(#{drop := _} = Spec)       -> error(badarg, [Spec]);
drop(#{})                       -> drop_r.

-spec timeout(#{timeout => Timeout}) -> NTimeout when
      Timeout :: timeout(),
      NTimeout :: timeout().
timeout(#{timeout := infinity}) ->
    infinity;
timeout(#{timeout := Timeout}) when is_integer(Timeout), Timeout >= 0 ->
    native(Timeout);
timeout(#{timeout := _} = Spec) ->
    error(badarg, [Spec]);
timeout(#{}) ->
    native(5000).

-spec sojourn_target(#{target => Target}) -> NTarget when
      Target :: non_neg_integer(),
      NTarget :: non_neg_integer().
sojourn_target(#{target := Target}) when is_integer(Target), Target >= 0 ->
    native(Target);
sojourn_target(#{target := _} = Spec) ->
    error(badarg, [Spec]);
sojourn_target(#{}) ->
    native(100).

-spec sojourn_target(Queue, #{Queue => #{target => Target}}) -> NTarget when
      Target :: non_neg_integer(),
      NTarget :: non_neg_integer().
sojourn_target(Queue, Spec) ->
    case Spec of
        #{Queue := #{target := Target}} when is_integer(Target), Target >= 0 ->
            native(Target);
        #{Queue := #{target := _}} ->
            error(badarg, [Queue, Spec]);
        #{Queue := #{}} ->
            native(100);
        #{Queue := _} ->
            error(badarg, [Queue, Spec]);
        #{} ->
            native(100)
    end.

-spec relative_target(#{target => Target}) -> NTarget when
      Target :: integer(),
      NTarget :: integer().
relative_target(#{target := Target}) when is_integer(Target) ->
    native(Target);
relative_target(#{target := _} = Spec) ->
    error(badarg, [Spec]);
relative_target(#{}) ->
    native(100).

-spec interval(#{interval => Interval}) -> NInterval when
      Interval :: pos_integer(),
      NInterval :: pos_integer().
interval(#{interval := Interval}) when is_integer(Interval), Interval > 0 ->
    native(Interval);
interval(#{interval := _} = Spec) ->
    error(badarg, [Spec]);
interval(#{}) ->
    native(1000).

-spec interval(Queue, #{Queue => #{interval => Interval}}) -> NInterval when
      Interval :: pos_integer(),
      NInterval :: pos_integer().
interval(Queue, Spec) ->
    case Spec of
        #{Queue := #{interval := Interval}}
          when is_integer(Interval), Interval > 0 ->
            native(Interval);
        #{Queue := #{interval := _}} ->
            error(badarg, [Queue, Spec]);
        #{Queue := #{}} ->
            native(1000);
        #{Queue := _} ->
            error(badarg, [Queue, Spec]);
        #{} ->
            native(1000)
    end.

-spec update(#{update => Update}) -> NUpdate when
      Update :: pos_integer(),
      NUpdate :: pos_integer().
update(#{update := Update}) when is_integer(Update), Update > 0 ->
    native(Update);
update(#{update := _} = Other) ->
    error(badarg, [Other]);
update(#{}) ->
    native(100).

-spec min_max(#{min => Min, max => Max}) -> {Min, Max} when
      Min :: non_neg_integer(),
      Max :: non_neg_integer() | infinity.
min_max(#{min := Min, max := Max})
  when is_integer(Min) andalso (Max == infinity orelse is_integer(Max)) andalso
       Min >= 0 andalso Max >= Min ->
    {Min, Max};
min_max(#{min := _, max := _} = Spec) ->
    error(badarg, [Spec]);
min_max(#{min := Min}) when is_integer(Min), Min >= 0 ->
    {Min, infinity};
min_max(#{min := _} = Spec) ->
    error(badarg, [Spec]);
min_max(#{max := Max})
  when Max == infinity orelse (is_integer(Max) andalso Max >= 0) ->
    {0, Max};
min_max(#{max := _} = Spec) ->
    error(badarg, [Spec]);
min_max(#{}) ->
    {0, infinity}.

-spec max(#{max => Max}) -> Max when
      Max :: non_neg_integer() | infinity.
max(#{max := infinity}) ->
    infinity;
max(#{max := Max}) when is_integer(Max), Max >= 0 ->
    Max;
max(#{max := _} = Spec) ->
    error(badarg, Spec);
max(#{}) ->
    infinity.

-spec upper(Queue, #{Queue => #{upper => Upper}}) -> NUpper when
      Upper :: non_neg_integer(),
      NUpper :: non_neg_integer().
upper(Queue, Spec) ->
    case Spec of
        #{Queue := #{upper := Upper}} when is_integer(Upper), Upper >= 0 ->
            native(Upper);
        #{Queue := #{upper := _}} ->
            error(badarg, [Queue, Spec]);
        #{Queue := #{}} ->
            native(5000);
        #{Queue := _} ->
            error(badarg, [Queue, Spec]);
        #{} ->
            native(5000)
    end.

-spec limit(#{limit => Limit}) -> Limit when
      Limit :: non_neg_integer().
limit(#{limit := Limit}) when is_integer(Limit), Limit >= 0 ->
    Limit;
limit(#{limit := _} = Spec) ->
    error(badarg, [Spec]);
limit(#{}) ->
    100.

-spec alarm(Type, #{alarm => Alarm}) -> NAlarm when
      Alarm :: any(),
      NAlarm :: {Type, pid()} | Alarm.
alarm(_, #{alarm := Alarm}) -> Alarm;
alarm(Type, #{})            -> {Type, self()}.

-spec uniform_interval_s(Interval, State) -> {CurrentInterval, NState} when
    Interval :: pos_integer(),
    State :: rand:state(),
    CurrentInterval :: pos_integer(),
    NState :: rand:state().
uniform_interval_s(Interval, State) ->
    {Int, NState} = rand:uniform_s(Interval, State),
    {(Interval div 2) + Int, NState}.

%% Internal

native(Time) ->
    erlang:convert_time_unit(Time, milli_seconds, native).
