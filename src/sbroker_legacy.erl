%%-------------------------------------------------------------------
%%
%% Copyright (c) 2015, James Fish <james@fishcakez.com>
%% Copyright Ericsson AB 2014-2015. All Rights Reserved
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
%% @doc This module provides a fallback monotonic time implementation using
%% `erlang:now/0' for OTP releases prior to 18.0.
-module(sbroker_legacy).

-behaviour(sbroker_time).

-compile(nowarn_deprecated_function).

-export([monotonic_time/0]).
-export([monotonic_time/1]).
-export([convert_time_unit/3]).

%% @doc Get the time, `Time', as an `integer()' in `micro_seconds'.
%%
%% Uses `erlang:now/0' to calculate a monotonically increasing time as an
%% `integer()'.
%%
%% @see erlang:now/0
-spec monotonic_time() -> Time when
      Time :: integer().
monotonic_time() ->
    {Mega, Sec, Micro} = erlang:now(),
    ((Mega * 1000000 + Sec) * 1000000) + Micro.

%% @doc Get the time, `Time', as an `integer()' in the `TimeUnit' time units.
%%
%% Uses `erlang:now/0' to calculate a monotonically increasing time as an
%% `integer()'.
%%
%% @see erlang:now/0
-spec monotonic_time(TimeUnit) -> Time when
      TimeUnit :: sbroker_time:unit(),
      Time :: integer().
monotonic_time(native) ->
    monotonic_time();
monotonic_time(nano_seconds) ->
    1000 * monotonic_time();
monotonic_time(micro_seconds) ->
    monotonic_time();
monotonic_time(milli_seconds) ->
    {Mega, Sec, Micro} = erlang:now(),
    (((Mega * 1000000) + Sec) * 1000) + (Micro div 1000);
monotonic_time(seconds) ->
    {Mega, Sec, _} = erlang:now(),
    (Mega * 1000000) + Sec;
monotonic_time(1000000) ->
    monotonic_time();
monotonic_time(N) when is_integer(N) and N > 0 ->
    (N * monotonic_time()) div 1000000;
monotonic_time(TimeUnit) ->
    error(badarg, [TimeUnit]).

%% @doc Convert the `integer()' time, `InTime', from `InTimeUnit' time units to
%% `OutTimeUnit' time units.
-spec convert_time_unit(InTime, InTimeUnit, OutTimeUnit) -> OutTime when
      InTime :: integer(),
      InTimeUnit :: sbroker_time:unit(),
      OutTimeUnit :: sbroker_time:unit(),
      OutTime :: integer().
convert_time_unit(InTime, InTimeUnit, OutTimeUnit) ->
    FU = integer_time_unit(InTimeUnit),
    TU = integer_time_unit(OutTimeUnit),
    case InTime < 0 of
        true -> TU*InTime - (FU - 1);
        false -> TU*InTime
    end div FU.

%% Internal

integer_time_unit(native) -> 1000*1000;
integer_time_unit(nano_seconds) -> 1000*1000*1000;
integer_time_unit(micro_seconds) -> 1000*1000;
integer_time_unit(milli_seconds) -> 1000;
integer_time_unit(seconds) -> 1;
integer_time_unit(I) when is_integer(I), I > 0 -> I;
integer_time_unit(BadRes) -> erlang:error(bad_time_unit, [BadRes]).
