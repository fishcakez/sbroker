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
-module(sbroker_timer).

%% public api

-export([start/1]).
-export([read/1]).
-export([restart/1]).
-export([config_change/2]).

%% types

-record(timer, {interval :: pos_integer(),
                next_timeout = 0 :: non_neg_integer(),
                ref = make_ref() :: reference()}).

-opaque timer() :: #timer{}.

-export_type([timer/0]).

%% public api

-spec start(Interval) -> {Time, Timer} when
      Interval :: pos_integer(),
      Time :: non_neg_integer(),
      Timer :: timer().
start(Interval) when is_integer(Interval) andalso Interval > 0 ->
    restart(#timer{interval=Interval}).

-spec read(Timer) -> {Time, NTimer} when
      Timer :: timer(),
      Time :: non_neg_integer(),
      NTimer :: timer().
read(#timer{ref=TRef, next_timeout=NextTimeout} = Timer) ->
    case erlang:read_timer(TRef) of
        false ->
            restart(Timer);
        Rem ->
            {NextTimeout-Rem, Timer}
    end.

-spec restart(Timer) -> {Time, NTimer} when
      Timer :: timer(),
      Time :: non_neg_integer(),
      NTimer :: timer().
restart(#timer{interval=Interval, next_timeout=Now} = Timer) ->
    TRef = gen_fsm:start_timer(Interval, ?MODULE) ,
    {Now, Timer#timer{next_timeout=Now+Interval, ref=TRef}}.

-spec config_change(Interval, Timer) -> {Time, NTimer} when
      Interval :: pos_integer(),
      Timer :: timer(),
      Time :: non_neg_integer(),
      NTimer :: timer().
config_change(Interval, #timer{interval=Interval, next_timeout=NextTimeout,
                               ref=TRef} = Timer)
  when is_integer(Interval) andalso Interval > 0 ->
    case erlang:read_timer(TRef) of
        false ->
            restart(Timer#timer{interval=Interval});
        Rem when Rem > Interval ->
            Now = NextTimeout - Rem,
            Result = restart(Timer#timer{interval=Interval, next_timeout=Now}),
            %% Cancel after as restart/1 may raise if Interval too big.
            _ = erlang:cancel_timer(TRef),
            Result;
        Rem ->
            {NextTimeout - Rem, Timer}
    end.
