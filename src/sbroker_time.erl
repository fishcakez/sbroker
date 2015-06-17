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
%% @doc This module provides a behaviour for reading the time and utility
%% functions to get the current time.
%%
%% The `sbroker_time' behaviour has two callbacks:
%% ```
%% -callback monotonic_time() :: Time :: integer().
%% -callback monotonic_time(TimeUnit :: unit()) -> Time :: integer().
%% '''
%% `monotonic_time/1' should return the time in the given time unit as an
%% integer. `monotonic_time/0' should be equivalent to `monotonic_time(native)'.
%%
%% The `TimeUnit' is a named time unit: `native', `nano_seconds',
%% `micro_seconds', `milli_seconds' or `seconds', or a `pos_integer()', which
%% is the integer increment for a second. For example `1000' is equivalent to
%% `milli_seconds' and `1000000' is equivalent to `micro_seconds'.
%%
%% Prior to 18.0, the `native' time units will always be `micro_seconds'. For
%% releases greater than and including 18.0 see the "Time and Time Correction"
%% chapter in the OTP documentation for more information. `sbroker' is multi
%% time warp safe and designed to be used with multi time warps.
-module(sbroker_time).

%% public api

-export([monotonic_time/0]).
-export([monotonic_time/1]).

%% types

-type unit() ::
    native | nano_seconds | micro_seconds | milli_seconds | seconds |
    pos_integer().

-export_type([unit/0]).

-callback monotonic_time() -> Time :: integer().
-callback monotonic_time(TimeUnit :: unit()) -> Time :: integer().

%% @doc Get the time, `Time', as an `integer()' in the `native' time units.
%%
%% Uses `erlang:monotonic_time/0' if it is exported, otherwise falls back to
%% `sbroker_legacy:monotonic_time/0'.
%%
%% @see erlang:monotonic_time/0
%% @see sbroker_legacy:monotonic_time/0
-spec monotonic_time() -> Time when
      Time :: integer().
monotonic_time() ->
    try erlang:monotonic_time() of
        Time ->
            Time
    catch
        error:undef ->
            sbroker_legacy:monotonic_time()
    end.

%% @doc Get the time, `Time', as an `integer()' in the `TimeUnit' time units.
%%
%% Use `erlang:monotonic_time/1' if it is exported, otherwise falls back to
%% `sbroker_legacy:monotonic_time/1'.
%%
%% @see erlang:monotonic_time/1
%% @see sbroker_legacy:monotonic_time/1
-spec monotonic_time(TimeUnit) -> Time when
      TimeUnit :: unit(),
      Time :: integer().
monotonic_time(TimeUnit) ->
    try erlang:monotonic_time(TimeUnit) of
        Time ->
            Time
    catch
        error:undef ->
            sbroker_legacy:monotonic_time(TimeUnit)
    end.
