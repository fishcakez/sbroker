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
%% @private
-module(sbroker_server_sup).

-behaviour(supervisor).

%% public API

-export([start_link/0]).

%% supervisor API

-export([init/1]).

%% public API

-spec start_link() -> {ok, Pid} when
      Pid :: pid().
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% supervisor API

init([]) ->
    BetterServer = {sbetter_server, {sbetter_server, start_link, []},
                    permanent, 5000, worker, [sbetter_server]},
    ProtectorServer = {sprotector_server, {sprotector_server, start_link, []},
                     permanent, 5000, worker, [sprotector_server]},
    {ok, {{one_for_one, 3, 30}, [BetterServer, ProtectorServer]}}.
