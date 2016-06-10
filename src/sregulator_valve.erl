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
%%      {Status :: open | closed, State :: any(),
%%       TimeoutTime :: integer() | infinity}.
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
%% `TimeoutTime' represents the next time a valve wishes to call
%% `handle_timeout/2' to change status. If a message is not received the timeout
%% should occur at or after `TimeoutTime'. The time must be greater than or
%% equal to `Time'. If a valve does not require a timeout then `TimeoutTime'
%% should be `infinity'.
%%
%% When allowing a request to run, `handle_ask/4':
%% ```
%% -callback handle_ask(Ref :: reference(), Pid :: pid(), Time :: integer(),
%%                      State :: any()) ->
%%      {go, Open, Status :: open | closed, NState :: any(),
%%       TimeoutTime :: integer() | infinity}.
%% '''
%% `Ref' is a monitor reference of the sender, `Pid', as in the `InternalMap'
%% in `init/3'.
%%
%% `Open' is the time the valve opened to allow this request.
%%
%% The other variables are equivalent to those in `init/3', with `NState' being
%% the new state.
%%
%% When a request has finished, `handle_done/3':
%% ```
%% -callback handle_done(Ref :: reference(), Time :: integer(),
%%                       State :: any()) ->
%%      {Result :: done | error, Status :: open | closed, NState :: any(),
%%       TimeoutTime :: integer() | infinity}.
%% '''
%% `Result' is `done' when the `Ref' is known by the valve and is removed, if
%% `Ref' is not found in the valve it is `error'.
%%
%% The other variables are equivalent to those in `handle_ask/4'.
%%
%% When a request is asking to continue, `handle_continue/3':
%% -callback handle_continue(Ref :: reference(), Time :: integer(),
%%                       State :: any()) ->
%%      {Result :: go, Open :: integer(), , Status :: open | closed,
%%       NState :: any(), TimeoutTime :: integer() | infinity} |
%%      {Result :: done | error, Status :: open | closed, NState :: any(),
%%       TimeoutTime :: integer() | infinity}.
%% '''
%%
%% `Result' is `go' if `Ref' is known by the valve and is allowed to
%% continue, if `Ref' is removed from the valve it is `done' and if `Ref' is not
%% found in the valve it is `error'.
%%
%% The other variables are equivalent to those in `handle_ask/3'.
%%
%% When handling a message, `handle_info/3':
%% ```
%% -callback handle_info(Msg :: any(), Time :: integer(), State :: any()) ->
%%     {Status :: open | closed, NState :: any(),
%%      TimeoutTime :: integer() | infinity}.
%% '''
%% `Msg' is the message, and may be intended for another callback.
%%
%% The other variables are equivalent to those in `init/3', with `NState' being
%% the new state.
%%
%% When a timeout occurs, `handle_timeout/2':
%% ```
%% -callback handle_timeout(Time :: integer(), State :: any()) ->
%%     {Status :: open | closed, NState :: any(),
%%      TimeoutTime :: integer() | infinity}.
%% '''
%% The variables are equivalent to those in `init/3', with `NState' being the
%% new state.
%%
%% When changing the state due to a code change, `code_change/4':
%% ```
%% -callback code_change(OldVsn :: any(), Time :: integer(), State :: any(),
%%                       Extra :: any()) ->
%%      {NState :: any(), TimeoutTime :: integer() | infinity}.
%% '''
%% On an upgrade `OldVsn' is version the state was created with and on an
%% downgrade is the same form except `{down, OldVsn}'. `OldVsn' is defined by
%% the vsn attribute(s) of the old version of the callback module. If no such
%% attribute is defined, the version is the checksum of the BEAM file. `Extra'
%% is from `{advanced, Extra}' in the update instructions.
%%
%% The other variables are equivalent to those in `init/3', with `NState' being
%% the new state.
%%
%% When changing the configuration of a valve, `config_change/4':
%% ```
%% -callback config_change(Args :: any(), Time :: integer(), State :: any()) ->
%%      {Status :: open | closed, NState :: any(),
%%       TimeoutTime :: integer() | infinity}.
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
%% When returning the open time for the time the valve opened or will open if
%% no side effects before then, `open_time/1':
%% ```
%% -callback open_time(State :: any()) -> OpenTime :: integer() | closed.
%% '''
%% `State' is the current state of the valve and `OpenTime' is the open time of
%% the valve, if closed and will not open without a side effect then `closed'.
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
-export([init/5]).
-export([code_change/6]).
-export([config_change/5]).
-export([terminate/3]).

%% types

-type internal_map() :: #{reference() => pid()}.

-export_type([internal_map/0]).

-callback init(Map :: internal_map(), Time :: integer(), Args :: any()) ->
    {Status :: open | closed, State :: any(),
     TimeoutTime :: integer() | infinity}.

-callback handle_ask(Pid :: pid(), Ref :: reference(), Time :: integer(),
                     State :: any()) ->
    {Result :: go, Open :: integer(), Status :: open | closed, NState :: any(),
     TimeoutTime :: integer() | infinity}.

-callback handle_done(Ref :: reference(), Time :: integer(), State :: any()) ->
    {Result :: done | error, Status :: open | closed, NState :: any(),
     TimeoutTime :: integer() | infinity}.

-callback handle_continue(Ref :: reference(), Time :: integer(),
                          State :: any()) ->
    {Result :: go, Open :: integer(), Status :: open | closed, NState :: any(),
     TimeoutTime :: integer() | infinity} |
    {Result :: done | error, Status :: open | closed, NState :: any(),
     TimeoutTime :: integer() | infinity}.

-callback handle_update(RelativeTime :: integer(), Time :: integer(),
                        State :: any()) ->
    {Status :: open | closed, NState :: any(),
     TimeoutTime :: integer() | infinity}.

-callback handle_info(Msg :: any(), Time :: integer(), State :: any()) ->
    {Status :: open | closed, NState :: any(),
     TimeoutTime :: integer() | infinity}.

-callback handle_timeout(Time :: integer(), State :: any()) ->
    {Status :: open | closed, NState :: any(),
     TimeoutTime :: integer() | infinity}.

-callback code_change(OldVsn :: any(), Time :: integer(), State :: any(),
                      Extra :: any()) ->
    {Status :: open | closed, NState :: any(),
     TimeoutTime :: integer() | infinity}.

-callback config_change(Args :: any(), Time :: integer(), State :: any()) ->
    {Status :: open | closed, NState :: any(),
     TimeoutTime :: integer() | infinity}.

-callback size(State :: any()) -> Size :: non_neg_integer().

-callback open_time(State :: any()) -> Open :: integer() | closed.

-callback terminate(Reason :: sbroker_handlers:reason(), State :: any()) ->
    Map :: internal_map().

%% sbroker_handlers api

%% @private
-spec initial_state() -> Map when
      Map :: internal_map().
initial_state() ->
    #{}.

%% @private
-spec init(Module, Map, Send, Time, Args) -> {{Status, State}, TimeoutTime} when
    Module :: module(),
    Map :: internal_map(),
    Send :: integer(),
    Time :: integer(),
    Args :: any(),
    Status :: open | closed,
    State :: any(),
    TimeoutTime :: integer() | infinity.
init(Mod, Map, Send, _, Args) ->
    {Status, State, TimeoutTime} = Mod:init(Map, Send, Args),
    {{Status, State}, TimeoutTime}.

%% @private
-spec code_change(Module, OldVsn, Send, Time, {Status, State}, Extra) ->
    {{NStatus, NState}, TimeoutTime} when
      Module :: module(),
      OldVsn :: any(),
      Send :: integer(),
      Time :: integer(),
      Status :: open | closed,
      State :: any(),
      Extra :: any(),
      NStatus :: open | closed,
      NState :: any(),
      TimeoutTime :: integer() | infinity.
code_change(Mod, OldVsn, Send, _, {_, State}, Extra) ->
    {Status, NState, TimeoutTime} = Mod:code_change(OldVsn, Send, State, Extra),
    {{Status, NState}, TimeoutTime}.

%% @private
-spec config_change(Module, Args, Send, Time, {Status, State}) ->
    {{NStatus, NState}, TimeoutTime} when
    Module :: module(),
    Args :: any(),
    Send :: integer(),
    Time :: integer(),
    Status :: open | closed,
    State :: any(),
    NStatus :: open | closed,
    NState :: any(),
    TimeoutTime :: integer() | infinity.
config_change(Mod, Args, Send, _, {_, State}) ->
    {Status, NState, TimeoutTime} = Mod:config_change(Args, Send, State),
    {{Status, NState}, TimeoutTime}.

%% @private
-spec terminate(Module, Reason, {Status, State}) -> Map when
      Module :: module(),
      Reason :: sbroker_handlers:reason(),
      Status :: open | closed,
      State :: any(),
      Map :: internal_map().
terminate(Mod, Reason, {_, State}) ->
    case Mod:terminate(Reason, State) of
        Map when is_map(Map) ->
            Map;
        Other ->
            exit({bad_return_value, Other})
    end.
