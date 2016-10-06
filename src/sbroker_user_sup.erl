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
-module(sbroker_user_sup).

-behaviour(supervisor).

%% public API

-export([start/2]).
-export([restart/2]).
-export([terminate/2]).
-export([delete/2]).
-export([which_children/1]).
-export([start_link/0]).

%% supervisor API

-export([init/1]).

%% public API

-spec start(Module, Name) -> {ok, Pid} | {error, Reason} when
      Module :: sbroker_user | sregulator_user,
      Name :: sbrokers:broker() | regulator:regulator(),
      Pid :: pid() | undefined,
      Reason :: term().
start(Module, Name) ->
    supervisor:start_child(?MODULE, child(Module, Name)).

-spec restart(Module, Name) -> {ok, Pid} | {error, Reason} when
      Module :: sbroker_user | sregulator_user,
      Name :: sbrokers:broker() | regulator:regulator(),
      Pid :: pid() | undefined,
      Reason :: term().
restart(Module, Name) ->
    supervisor:restart_child(?MODULE, {Module, Name}).

-spec terminate(Module, Name) -> ok | {error, not_found} when
      Module :: sbroker_user | sregulator_user,
      Name :: sbrokers:broker() | regulator:regulator().
terminate(Module, Name) ->
    supervisor:terminate_child(?MODULE, {Module, Name}).

-spec delete(Module, Name) -> ok | {error, Reason} when
      Module :: sbroker_user | sregulator_user,
      Name :: sbrokers:broker() | regulator:regulator(),
      Reason :: running | restarting | not_found.
delete(Module, Name) ->
    supervisor:delete_child(?MODULE, {Module, Name}).

-spec which_children(Module) -> [{Name, Pid, Type, Modules}] when
      Module :: sbroker_user | sregulator_user,
      Name :: sbroker:name() | sregulator:name(),
      Pid :: undefined | pid(),
      Type :: worker,
      Modules :: dynamic.
which_children(Module) ->
    [{Name, Pid, Type, Modules} ||
     {{Mod, Name}, Pid, Type, Modules} <- supervisor:which_children(?MODULE),
      Mod == Module].

-spec start_link() -> {ok, Pid} when
      Pid :: pid().
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% supervisor API

init([]) ->
    {ok, {{one_for_one, 3, 30}, children()}}.

%% internal

children() ->
    Env = application:get_all_env(sbroker),
    Brokers = proplists:get_value(brokers, Env, []),
    Regulators = proplists:get_value(regulators, Env, []),
    brokers(Brokers) ++ regulators(Regulators).

brokers(Brokers) ->
    children(sbroker_user, Brokers).

regulators(Regulators) ->
    children(sregulator_user, Regulators).

children(Module, List) ->
    [child(Module, Name) || {Name, _} <- List].

child(Module, Name) ->
    {{Module, Name}, {Module, start_link, [Name]},
     permanent, 5000, worker, dynamic}.
