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
-module(sbetter_statem_meter).

-behaviour(sbroker_meter).

-export([init/2]).
-export([handle_update/5]).
-export([handle_info/3]).
-export([code_change/4]).
-export([config_change/3]).
-export([terminate/2]).

init(Time, {Pid, Arg}) ->
    {State, Next} = sbetter_meter:init(Time, Arg),
    {{Pid, State}, Next}.

handle_update(QueueDelay, ProcessDelay, RelativeTime, Time, {Pid, State}) ->
    {NState, Next} = sbetter_meter:handle_update(QueueDelay, ProcessDelay,
                                                 RelativeTime, Time, State),
    Pid ! {meter, QueueDelay, ProcessDelay, RelativeTime, Time},
    {{Pid, NState}, Next}.

handle_info(Msg, Time, {Pid, State}) ->
    {NState, Next} = sbetter_meter:handle_info(Msg, Time, State),
    {{Pid, NState}, Next}.

code_change(Msg, Time, {Pid, State}, Extra) ->
    {NState, Next} = sbetter_meter:code_change(Msg, Time, State, Extra),
    {{Pid, NState}, Next}.

config_change({Pid, Arg}, Time, {Pid, State}) ->
    {NState, Next} = sbetter_meter:config_change(Arg, Time, State),
    {{Pid, NState}, Next}.

terminate(Reason, {_, State}) ->
    sbetter_meter:terminate(Reason, State).
