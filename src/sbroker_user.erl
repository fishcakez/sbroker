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
%% @doc This module provides helpers for managing `sbroker' processes that are
%% automatically started in the `sbroker' supervision tree.
%%
%% `brokers' can be automatically started when the `sbroker' application starts
%% by setting the `sbroker' application env `brokers'. It is a list of broker
%% names and specifications, of the form:
%% ```
%% [{Name :: sbroker:name(),
%%   Spec :: {AskQueueSpec :: sbroker:handler_spec(),
%%            AskRQueueSpec :: sbroker:handler_spec(),
%%            [MeterSpec :: sbroker:handler_spec()]}}]
%% '''
%% `Name' is the name of the broker and each broker will be registered using its
%% name, e.g. `{local, my_broker}'. `Spec' is the specification for the
%% `sbroker', which is equivalent to returning `{ok, Spec}' from the `init/1'
%% callback: `AskQueueSpec' is the `ask' queue spec, `AskRQueueSpec' is the
%% `ask_r' queue spec and `MeterSpec' is a meter spec.
%%
%% Starting brokers in the `sbroker' application allows multiple applications,
%% or parts of the same supervision tree, to access a `sbroker' even if their
%% counterparty crashes. It also allows either party to be started first and
%% await the other using the `sbroker' queues.
-module(sbroker_user).

-behaviour(sbroker).

%% public API

-export([start/1]).
-export([restart/1]).
-export([terminate/1]).
-export([delete/1]).
-export([which_brokers/0]).
-export([change_config/0]).
-export([start_link/1]).

%% sbroker API

-export([init/1]).

%% public API

%% @doc Starts a broker with name, `Name'.
%%
%% The broker is started in the `sbroker' application's supervision tree using
%% configuration from the `sbroker' application env `brokers'. Brokers are
%% automatically started when the `sbroker' application is started so this
%% function should only be required if a broker is added to the configuration.
%%
%% Returns `{ok, Pid}' on starting the broker, where `Pid' is the `pid()' of the
%% broker, `{ok, undefined}' if a configuration for `Name' does not exist in
%% `brokers' or `{error, Reason}' if the broker fails to start with reason
%% `Reason'.
-spec start(Name) -> {ok, Pid} | {error, Reason} when
      Name :: sbroker:broker(),
      Pid :: pid() | undefined,
      Reason :: term().
start(Name) ->
      sbroker_user_sup:start(?MODULE, Name).

%% @doc Restart a broker with name, `Name'.
%%
%% The broker is restarted in the `sbroker' application's supervision tree using
%% configuration from the `sbroker' application env `brokers'. Brokers are
%% automatically started when the `sbroker' is started so this function should
%% only be required if a broker is terminated.
%%
%% Returns `{ok, Pid}' on starting the broker, where `Pid' is the `pid()' of the
%% broker, `{ok, undefined}' if a configuration for `Name' does not exist in
%% `brokers' or `{error, Reason}' if the broker fails to start with reason
%% `Reason'.
-spec restart(Name) -> {ok, Pid} | {error, Reason} when
      Name :: sbroker:broker(),
      Pid :: pid() | undefined,
      Reason :: term().
restart(Name) ->
      sbroker_user_sup:restart(?MODULE, Name).

%% @doc Terminate a broker with name, `Name'.
%%
%% The broker is terminated in the `sbroker' application's supervision tree.
%% Brokers are automatically started when the `sbroker' is started, and might be
%% restarted if the entry remains in the `sbroker' application env `brokers', so
%% this function should only be required to terminate a broker after it has been
%% removed from `brokers'.
%%
%% Returns `ok' once the broker is terminated, otherwise `{error, not_found}'.
-spec terminate(Name) -> ok | {error, not_found} when
      Name :: sbroker:broker().
terminate(Name) ->
      sbroker_user_sup:terminate(?MODULE, Name).

%% @doc Delete a broker with name, `Name'.
%%
%% The broker is deleted in the `sbroker' application's supervision tree using
%% Brokers are automatically started when the `sbroker' is started, and might be
%% restarted if the entry remains in the `sbroker' application env `brokers', so
%% this function should only be required to delete a broker after it has removed
%% from `brokers'.
%%
%% Returns `ok' on successfully deleting the regulator, otherwise
%% `{error, Reason}' where `Reason' is reason for the error.
-spec delete(Name) -> ok | {error, Reason} when
      Name :: sbroker:broker(),
      Reason :: running | restarting | not_found.
delete(Name) ->
      sbroker_user_sup:delete(?MODULE, Name).

%% @doc List user brokers started in the `sbroker' application.
%%
%% Returns a list of brokers where `Name' is the `sbroker:name()' of the broker,
%% `Pid' is the `pid()' or `undefined', `Type' is `worker' and `Modules' is
%% `dynamic'.
-spec which_brokers() -> [{Name, Pid, Type, Modules}] when
      Name :: sbroker:name(),
      Pid :: undefined | pid(),
      Type :: worker,
      Modules :: dynamic.
which_brokers() ->
    sbroker_user_sup:which_children(?MODULE).

%% @doc Call `sbroker:change_config/1' on all brokers started in the `sbroker'
%% application.
%%
%% This function can be used to reconfigure all brokers that are already started
%% after the `sbroker' application env `brokers' is changed.
%%
%% Returns a list of failed changes, where `Name' is the `sbroker:name()' of the
%% broker and `Reason' is the reason.
-spec change_config() -> [{Name, Reason}] when
      Name :: sbroker:name(),
      Reason :: term().
change_config() ->
    [{Name, Reason} ||
     {Name, Pid, _, _} <- which_brokers(), is_pid(Pid),
     {error, Reason} <- [change_config(Pid)]].

%% @private
-spec start_link(Name) -> {ok, Pid} | {error, Reason} when
      Name :: sbroker:name(),
      Pid :: pid(),
      Reason :: term().
start_link(Name) ->
    sbroker:start_link(Name, ?MODULE, Name, []).

%% sbroker API

%% @private
init(Name) ->
    Brokers = application:get_env(sbroker, brokers, []),
    case lists:keyfind(Name, 1, Brokers) of
        {_, Config} -> {ok, Config};
        false       -> ignore
    end.

%% internal

change_config(Pid) ->
    try sbroker:change_config(Pid) of
        Result ->
            Result
    catch
        exit:Reason ->
            % crashed broker will be restarted with new config or take down
            % the supervisor if it fails.
            {exit, Reason}
    end.
