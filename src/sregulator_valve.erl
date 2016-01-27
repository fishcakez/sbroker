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
%% @doc Behaviour for implementing values for `sregulator'.
%%
%% A custom valve must implement the `sregulator_valve' behaviour. The first
%% callback is `init/3', which starts the valve:
%% ```
%% -callback init(InternalMap :: internal_map(), Time :: integer(),
%%                Args :: any()) ->
%%      {Status :: open | closed, State :: any()}.
%% '''
%% `InternalMap' is the internal map of running processes, it is a `map()' with
%% monitor `reference()' keys and `pid()' value where the monitor is that of the
%% process.
%%
%% `Time' is the time, in `native' time units, of the valve at creation. Some
%% other callbacks will receive the current time of the valve as the second last
%% argument. It is monotically increasing, so subsequent calls will have the
%% same or a greater time.
%%
%% `Args' is the arguments for the valve. It can be any term.
%%
%% `Status' is whether the valve accepts new requests or not. The
%% `handle_ask/4' callback to handle a request will only be called when the
%% previous callback returns `open'.
%%
%% `State' is the state of the queue and used in the next call.
%%
%% When allowing a request to run, `handle_ask/4':
%% ```
%% -callback handle_ask(Ref :: reference() Pid :: pid(), Time :: integer(),
%%                      State :: any()) ->
%%      {Status :: open | closed, NState :: any()}.
%% '''
%% `Ref' is a monitor reference of the sender, `Pid', as in the `InternalMap'
%% in `init/3'.
%%
%% The other variables are equivalent to those in `init/3', with `NState' being
%% the new state.
%%
%% When a request has finished, `handle_done/3':
%% ```
%% -callback handle_done(Ref :: reference(), Time :: integer(),
%%                       State :: any()) ->
%%      {Result :: done | error, Status :: open | closed, NState :: any()}.
%% '''
%% `Result' is `done' when the `Ref' is known by the valve and is removed, if
%% `Ref' is not found in the valve it is `error'.
%%
%% The other variables are equivalent to those in `handle_ask/4'.
%%
%% When a request is asking to continue, `handle_continue/3':
%% -callback handle_continue(Ref :: reference(), Time :: integer(),
%%                       State :: any()) ->
%%      {Result :: continue | done | error, Status :: open | closed,
%%       NState :: any()}.
%% '''
%% `Result' is `continue' if `Ref' is known by the valve and is allowed to
%% continue, if `Ref' is removed from the valve it is `done' and if `Ref' is not
%% found in the valve it is `error'.
%%
%% The other variables are equivalent to those in `handle_ask/3'.
%% When handling a message, `handle_info/3':
%% ```
%% -callback handle_info(Msg :: any(), Time :: integer(), State :: any()) ->
%%     {Status :: open | closedm NState :: any()}.
%% '''
%% `Msg' is the message, and may be intended for another callback.
%%
%% The other variables are equivalent to those in `init/3', with `NState' being
%% the new state.
%%
%% When changing the configuration of a valve, `config_change/4':
%% ```
%% -callback config_change(Args :: any(), Time :: integer(), State :: any()) ->
%%      {Status :: open | closed, NState :: any()}.
%% '''
%% The variables are equivalent to those in `init/3', with `NState' being the
%% new state.
%%
%% When returning the number of tasks monitored by the valve, `size/1':
%% ```
%% -callback size(State :: any()) -> Size :: non_neg_integer().
%% '''
%%
%% `State' is the current state of the valve and `Size' is the number of tasks
%% being monitorred. This callback must be idempotent and not change the status
%% of the valve.
%%
%% When cleaning up the valve, `terminate/2':
%% ```
%% -callback terminate(Reason :: sbroker_handlers:reason(), State :: any()) ->
%%      InternalMap :: internal_map().
%% '''
%% `Reason' is `stop' if the valve is being shutdown, `change' if the valve is
%% being replaced by another valve, `{bad_return_value, Return}' if a previous
%% callback returned an invalid term or `{Class, Reason, Stack}' if a previous
%% callback raised an exception.
%%
%% `State' is the current state of the valve.
%%
%% `InternalMap' is the same as `init/3' and is passed to the next valve if
%% `Reason' is `change'.
%%
%% The process controlling the valve may not be terminating with the valvee and
%% so `terminate/2' should do any clean up required.
-module(sregulator_valve).

-behaviour(sbroker_handlers).

%% sbroker_handlers api

-export([initial_state/0]).
-export([init/4]).
-export([config_change/4]).
-export([terminate/3]).

%% types

-type internal_map() :: #{reference() => pid()}.

-export_type([internal_map/0]).

-callback init(Map :: internal_map(), Time :: integer(), Args :: any()) ->
    {Status :: open | closed, State :: any()}.

-callback handle_ask(Pid :: pid(), Ref :: reference(), Time :: integer(),
                     State :: any()) ->
    {Status :: open | closed, NState :: any()}.

-callback handle_done(Ref :: reference(), Time :: integer(), State :: any()) ->
    {Result :: done | error, Status :: open | closed, NState :: any()}.

-callback handle_continue(Ref :: reference(), Time :: integer(),
                          State :: any()) ->
    {Result :: continue | done | error, Status :: open | closed,
     NState :: any()}.

-callback handle_update(RelativeTime :: integer(), Time :: integer(),
                        State :: any()) ->
    {Status :: open | closed, NState :: any()}.

-callback handle_info(Msg :: any(), Time :: integer(), State :: any()) ->
    {Status :: open | closed, NState :: any()}.

-callback config_change(Args :: any(), Time :: integer(), State :: any()) ->
    {Status :: open | closed, NState :: any()}.

-callback size(State :: any()) -> Size :: non_neg_integer().

-callback terminate(Reason :: sbroker_handler:reason(), State :: any()) ->
    Map :: internal_map().

%% sbroker_handlers api

%% @private
-spec initial_state() -> Map when
      Map :: internal_map().
initial_state() ->
    #{}.

%% @private
-spec init(Module, Map, Time, Args) -> {{Status, State}, infinity} when
    Module :: module(),
    Map :: internal_map(),
    Time :: integer(),
    Args :: any(),
    Status :: open | closed,
    State :: any().
init(Mod, Map, Now, Args) ->
    {Mod:init(Map, Now, Args), infinity}.

%% @private
-spec config_change(Module, Args, Time, State) ->
    {{Status, NState}, infinity} when
    Module :: module(),
    Args :: any(),
    Time :: integer(),
    State :: any(),
    Status :: open | closed,
    NState :: any().
config_change(Mod, Args, Now, State) ->
    {Mod:config_change(Args, Now, State), infinity}.

%% @private
-spec terminate(Module, Reason, State) -> Map when
      Module :: module(),
      Reason :: sbroker_handlers:reason(),
      State :: any(),
      Map :: internal_map().
terminate(Mod, Reason, State) ->
    case Mod:terminate(Reason, State) of
        Map when is_map(Map) ->
            Map;
        Other ->
            exit({bad_return_value, Other})
    end.
