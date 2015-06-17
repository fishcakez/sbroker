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
%% @doc This module provides a fallback monotonic time implementation using
%% `erlang:now/0' for OTP releases prior to 18.0.
-module(sbroker_legacy).

-behaviour(sbroker_time).

-compile(nowarn_deprecated_function).

-export([monotonic_time/0]).
-export([monotonic_time/1]).

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
    (N * monotonic_time()) div 1000000.
