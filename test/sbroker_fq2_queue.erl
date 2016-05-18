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
-module(sbroker_fq2_queue).

-behaviour(sbroker_queue).
-behaviour(sbroker_fair_queue).

-export([init/3]).
-export([handle_in/5]).
-export([handle_out/2]).
-export([handle_fq_out/2]).
-export([handle_timeout/2]).
-export([handle_cancel/3]).
-export([handle_info/3]).
-export([config_change/3]).
-export([len/1]).
-export([terminate/2]).

%% This sbroker_queue module is used to an alternate to sbroker_fq_queue
%% to test config changes.

init(Q, Time, Args) ->
    sbroker_fq_queue:init(Q, Time, Args).

handle_in(SendTime, From, Value, Time, State) ->
    sbroker_fq_queue:handle_in(SendTime, From, Value, Time, State).

handle_out(Time, State) ->
    sbroker_fq_queue:handle_out(Time, State).

handle_fq_out(Time, State) ->
    sbroker_fq_queue:handle_fq_out(Time, State).

handle_timeout(Time, State) ->
    sbroker_fq_queue:handle_timeout(Time, State).

handle_cancel(Tag, Time, State) ->
    sbroker_fq_queue:handle_cancel(Tag, Time, State).

handle_info(Msg, Time, State) ->
    sbroker_fq_queue:handle_info(Msg, Time, State).

config_change(Args, Time, State) ->
    sbroker_fq_queue:config_change(Args, Time, State).

len(State) ->
    sbroker_fq_queue:len(State).

terminate(Reason, State) ->
    sbroker_fq_queue:terminate(Reason, State).
