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
%% @doc Behaviour for implementing meters for `sbroker' and `sregulator'.
%%
%% A custom meter must implement the `sbroker_meter' behaviour. The first
%% callback is `init/2', which starts the meter:
%% ```
%% -callback init(Time :: integer(), Args :: any()) ->
%%      {State :: any(), UpdateTime :: integer() | infinity}.
%% '''
%% `Time' is the time, in `native' time units, of the meter at creation. Some
%% other callbacks will receive the current time of the meter as the second last
%% argument. It is monotically increasing, so subsequent calls will have the
%% same or a greater time.
%%
%% `Args' is the arguments for the meter. It can be any term.
%%
%% `State' is the state of the meter and used in the next call.
%%
%% `UpdateTime' represents the next time a meter wishes to call
%% `handle_update/4' to update itself. If a message is not recevued the update
%% should occur at or after `UpdateTime'. The time must be greater than or equal
%% to `Time'. If a meter does not require an update then `UpdateTime' should be
%% `infinity'.
%%
%% When updating the meter, `handle_update/5':
%% ```
%% -callback handle_update(QueueDelay :: non_neg_integer(),
%%                         ProcessDelay :: non_neg_integer(),
%%                         RelativeTime :: integer(), Time :: integer(),
%%                         State :: any()) ->
%%      {NState :: any(), UpdateTime :: integer() | infinity}.
%% '''
%% `QueueDelay' is the approximate time a message spends in the message queue of
%% the process. `ProcessDelay' is the average time spent processing a message
%% since the last update. `RelativeTime' is an approximation of the
%% `RelativeTime' for an `ask' request if a match was to occur immediately. If
%% the process has not matched a request for a significant period of time this
%% value can grow large and become inaccurate.
%%
%% The other variables are equivalent to those in `init/2', with `NState' being
%% the new state.
%%
%% When handling a message, `handle_info/3':
%% ```
%% -callback handle_info(Msg :: any(), Time :: integer(), State :: any()) ->
%%     {NState :: any(), TimeoutTime :: integer() | infinity}.
%% '''
%% `Msg' is the message, and may be intended for another callback.
%%
%% The other variables are equivalent to those in `init/2', with `NState' being
%% the new state.
%%
%% The other variables are equivalent to those in `init/3', with `NState' being
%% the new state.
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
%% When changing the configuration of a queue, `config_change/4':
%% ```
%% -callback config_change(Args :: any(), Time :: integer(), State :: any()) ->
%%      {NState :: any(), TimeoutTime :: integer() | infinity}.
%% '''
%% The variables are equivalent to those in `init/2', with `NState' being the
%% new state.
%%
%% When cleaning up the meter, `terminate/2':
%% ```
%% -callback terminate(Reason :: sbroker_handlers:reason(), State :: any()) ->
%%      any().
%% '''
%% `Reason' is `stop' if the meter is being shutdown, `change' if the meter is
%% being replaced by another meter, `{bad_return_value, Return}' if a previous
%% callback returned an invalid term or `{Class, Reason, Stack}' if a previous
%% callback raised an exception.
%%
%% `State' is the current state of the meter.
%%
%% The return value is ignored.
-module(sbroker_meter).

%% private api

-export([code_change/6]).
-export([terminate/3]).

%% types

-callback init(Time :: integer(), Args :: any()) ->
    {State :: any(), UpdateTime :: integer() | infinity}.

-callback handle_update(QueueDelay :: non_neg_integer(),
                        ProcessDelay :: non_neg_integer(),
                        RelativeTime :: integer(), Time :: integer(),
                        State :: any()) ->
    {NState :: any(), UpdateTime :: integer() | infinity}.

-callback handle_info(Msg :: any(), Time :: integer(), State :: any()) ->
    {NState :: any(), UpdateTime :: integer() | infinity}.

-callback code_change(OldVsn :: any(), Time :: integer(), State :: any(),
                      Extra :: any()) ->
    {NState :: any(), TimeoutTime :: integer() | infinity}.

-callback config_change(Args :: any(), Time :: integer(), State :: any()) ->
    {NState :: any(), UpdateTime :: integer() | infinity}.

-callback terminate(Reason :: sbroker_handlers:reason(), State :: any()) ->
    any().

%% private api

%% @private
-spec code_change(Module, OldVsn, Send, Time, State, Extra) ->
    {NState, TimeoutTime} when
      Module :: module(),
      OldVsn :: any(),
      Send :: integer(),
      Time :: integer(),
      State :: any(),
      Extra :: any(),
      NState :: any(),
      TimeoutTime :: integer() | infinity.
code_change(Mod, OldVsn, _, Time, State, Extra) ->
    Mod:code_change(OldVsn, Time, State, Extra).

%% @private
-spec terminate(Module, Reason, State) -> any() when
      Module :: module(),
      Reason :: sbroker_handlers:reason(),
      State :: any().
terminate(Mod, Reason, State) ->
    Mod:terminate(Reason, State).
