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
%% @doc This module provides time utility functions.
%%
%% `sbroker' and `sregulator' use the `native' time units of the VM to measure
%% time. Conversion functions to and from milliseconds are provided as a
%% convenience.
%%
%% Prior to 18.0, the `native' time units will always be microseconds. For
%% releases greater than and including 18.0 see the "Time and Time Correction"
%% chapter in the OTP documentation for more information. `sbroker' and
%% `sregulator' are multi time warp safe and designed to be used with multi
%% time warps.
-module(sbroker_time).

-compile(nowarn_deprecated_function).

-export([native/0]).
-export([milli_seconds/0]).
-export([native_to_milli_seconds/1]).
-export([milli_seconds_to_native/1]).

%% @doc Get the time, `Native', as an `integer()' in the `native' time units.
-spec native() -> Native when
      Native :: integer().
native() ->
    try erlang:monotonic_time() of
        Native ->
            Native
    catch
        error:undef ->
            {Mega, Sec, Micro} = erlang:now(),
            ((Mega * 1000000 + Sec) * 1000000) + Micro
    end.

%% @doc Get the time, `MilliSeconds', as an `integer()' in milliseconds.
-spec milli_seconds() -> MilliSeconds when
      MilliSeconds :: integer().
milli_seconds() ->
    try erlang:monotonic_time(milli_seconds) of
        MilliSeconds ->
            MilliSeconds
    catch
        error:undef ->
            {Mega, Sec, Micro} = erlang:now(),
            ((Mega * 1000000 + Sec) * 1000) + (Micro div 1000)
    end.

%% @doc Convert a time, `Native', in `native' time units to milliseconds,
%% `MilliSeconds'.
-spec native_to_milli_seconds(Native) -> MilliSeconds when
      Native :: integer(),
      MilliSeconds :: integer().
native_to_milli_seconds(Native) ->
    try erlang:convert_time_unit(Native, native, milli_seconds) of
        MilliSeconds ->
            MilliSeconds
    catch
        error:undef ->
            Native div 1000
    end.

%% @doc Convert a time, `MilliSeconds', in milliseconds to `native' time units,
%% `Native'.
-spec milli_seconds_to_native(MilliSeconds) -> Native when
      Native :: integer(),
      MilliSeconds :: integer().
milli_seconds_to_native(MilliSeconds) ->
    try erlang:convert_time_unit(MilliSeconds, milli_seconds, native) of
        Time ->
            Time
    catch
        error:undef ->
            MilliSeconds * 1000
    end.
