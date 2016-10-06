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
%% @doc This module provides helpers for managing `sregulator' processes that
%% are automatically started in the `sbroker' supervision tree.
%%
%% `sregulator' processes can be automatically started when the `sbroker'
%% application starts by setting the `sbroker' application env `regulators'. It
%% is a list of regulator names and specifications, of the form:
%% ```
%% [{Name :: sregulator:name(),
%%   Spec :: {QueueSpec :: sregulator:handler_spec(),
%%            ValveSpec :: sregulator:handler_spec(),
%%            [MeterSpec :: sregulator:handler_spec()]}}]
%% '''
%% `Name' is the name of the regulator and each regulator will be registered
%% using its name, e.g. `{local, my_regulator}'. `Spec' is the specification for
%% the `sregulator', which is equivalent to returning `{ok, Spec}' from the
%% `init/1' callback: `QueueSpec' is the queue spec, `ValveSpec' is the valve
%% spec and `MeterSpec' is a meter spec.
%%
%% Starting regulators in the `sbroker' application allows multiple
%% applications, to access a `sregulator' or to isolate the regulator from
%% errors in an application.
-module(sregulator_user).

-behaviour(sregulator).

%% public API

-export([start/1]).
-export([restart/1]).
-export([terminate/1]).
-export([delete/1]).
-export([which_regulators/0]).
-export([change_config/0]).
-export([start_link/1]).

%% sregulator API

-export([init/1]).

%% public API

%% @doc Starts a regulator with name, `Name'.
%%
%% The regulator is started in the `sbroker' application's supervision tree
%% using configuration from the `sbroker' application env `regulators'.
%% Regulators are automatically started when the `sbroker' application is
%% started so this function should only be required if a regulator is added to
%% the configuration.
%%
%% Returns `{ok, Pid}' on starting the regulator, where `Pid' is the `pid()' of
%% the regulator, `{ok, undefined}' if a configuration for `Name' does not exist
%% in `regulators' or `{error, Reason}' if the regulator fails to start with
%% reason `Reason'.
-spec start(Name) -> {ok, Pid} | {error, Reason} when
      Name :: sregulator:regulator(),
      Pid :: pid() | undefined,
      Reason :: term().
start(Name) ->
      sbroker_user_sup:start(?MODULE, Name).

%% @doc Restart a regulator with name, `Name'.
%%
%% The regulator is restarted in the `sbroker' application's supervision tree
%% using configuration from the `sbroker' application env `regulators'.
%% Regulators are automatically started when the `sbroker' is started so this
%% function should only be required if a regulator is terminated.
%%
%% Returns `{ok, Pid}' on starting the regulator, where `Pid' is the `pid()' of
%% the regulator, `{ok, undefined}' if a configuration for `Name' does not exist
%% in `regulators' or `{error, Reason}' if the regulator fails to start with
%% reason `Reason'.
-spec restart(Name) -> {ok, Pid} | {error, Reason} when
      Name :: sregulator:regulator(),
      Pid :: pid() | undefined,
      Reason :: term().
restart(Name) ->
      sbroker_user_sup:restart(?MODULE, Name).

%% @doc Terminate a regulator with name, `Name'.
%%
%% The regulator is terminated in the `sbroker' application's supervision tree.
%% Regulators are automatically started when the `sbroker' is started, and might
%% be restarted if the entry remains in the `sbroker' application env
%% `regulators', so this function should only be required to terminate a
%% regulator after it has been removed from `regulators'.
%%
%% Returns `ok' once the regulator is terminated, otherwise
%% `{error, not_found}'.
-spec terminate(Name) -> ok | {error, not_found} when
      Name :: sregulator:regulator().
terminate(Name) ->
      sbroker_user_sup:terminate(?MODULE, Name).

%% @doc Delete a regulator with name, `Name'.
%%
%% The regulator is deleted in the `sbroker' application's supervision tree.
%% Regulators are automatically started when the `sbroker' is started, and might
%% be restarted if the entry remains in the `sbroker' application env
%% `regulators', so this function should only be required to delete a
%% regulator after it has been removed from `regulators'.
%%
%% Returns `ok' on successfully deleting the regulator, otherwise
%% `{error, Reason}' where `Reason' is reason for the error.
-spec delete(Name) -> ok | {error, Reason} when
      Name :: sregulator:regulator(),
      Reason :: running | restarting | not_found.
delete(Name) ->
      sbroker_user_sup:delete(?MODULE, Name).

%% @doc List user regulators started in the `sbroker' application.
%%
%% Returns a list of regulators where `Name' is the `sregulator:name()' of the
%% regulator, `Pid' is the `pid()' or `undefined', `Type' is `worker' and
%% `Modules' is `dynamic'.
-spec which_regulators() -> [{Name, Pid, Type, Modules}] when
      Name :: sregulator:name(),
      Pid :: undefined | pid(),
      Type :: worker,
      Modules :: dynamic.
which_regulators() ->
    sbroker_user_sup:which_children(?MODULE).

%% @doc Call `sregulator:change_config/1' on all regulators started in the
%% `sbroker' application.
%%
%% This function can be used to reconfigure all regulators that are already
%% started after the `sbroker' application env `regulators' is changed.
%%
%% Returns a list of failed changes, where `Name' is the `sregulator:name()' of
%% the regulator and `Reason' is the reason.
-spec change_config() -> [{Name, Reason}] when
      Name :: sregulator:name(),
      Reason :: term().
change_config() ->
    [{Name, Reason} ||
     {Name, Pid, _, _} <- which_regulators(), is_pid(Pid),
     {error, Reason} <- [change_config(Pid)]].

%% @private
-spec start_link(Name) -> {ok, Pid} | {error, Reason} when
      Name :: sregulator:name(),
      Pid :: pid(),
      Reason :: term().
start_link(Name) ->
    sregulator:start_link(Name, ?MODULE, Name, []).

%% sregulator API

%% @private
init(Name) ->
    Regulators = application:get_env(sbroker, regulators, []),
    case lists:keyfind(Name, 1, Regulators) of
        {_, Config} -> {ok, Config};
        false       -> ignore
    end.

%% internal

change_config(Pid) ->
    try sregulator:change_config(Pid) of
        Result ->
            Result
    catch
        exit:Reason ->
            % crashed regulator will be restarted with new config or take down
            % the supervisor if it fails.
            {exit, Reason}
    end.
