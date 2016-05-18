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
-module(sbroker_test).

-behaviour(sbroker).

%% public api

-export([start_link/0]).

%% sbroker api

-export([init/1]).

%% public api

-spec start_link() -> {ok, Pid} when
      Pid :: pid().
start_link() ->
    sbroker:start_link(?MODULE, undefined, [{read_time_after, 2}]).

%% sbroker api

init(undefined) ->
    QSpec = {sbroker_timeout_queue, {out, 200, drop, 0, infinity}},
    MSpec = {sbroker_timeout_meter, infinity},
    {ok, {QSpec, QSpec, MSpec}}.
