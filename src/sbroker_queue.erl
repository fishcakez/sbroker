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
%% @doc Behaviour for implementing queues for `sbroker' and `sregulator'.
%%
%% A custom queue must implement the `sbroker_queue' behaviour. The first
%% callback is `init/3', which starts the queue:
%% ```
%% -callback init(InternalQueue :: internal_queue(), Time :: integer(),
%%                Args :: any()) ->
%%      {State :: any(), TimeoutTime :: integer() | infinity}.
%% '''
%% `InternalQueue' is the internal queue of requests, it is a `queue:queue()'
%% with items of the form `{SendTime, From, Value, Reference}'. `SendTime' is
%% the approximate time the request was sent in `native' time units and is
%% always less than or equal to `Time'.`From' is the a 2-tuple containing the
%% senders pid and a response tag. `SendTime' and `From' can be used with
%% `drop/3' to drop a request. `Value' is any term, `Reference' is the monitor
%% reference of the sender.
%%
%% `Time' is the time, in `native' time units, of the queue at creation. Some
%% other callbacks will receive the current time of the queue as the second last
%% argument. It is monotically increasing, so subsequent calls will have the
%% same or a greater time.
%%
%% `Args' is the arguments for the queue. It can be any term.
%%
%% `State' is the state of the queue and used in the next call.
%%
%% `TimeoutTime' represents the next time a queue wishes to call
%% `handle_timeout/2' to drop items. If a message is not received the timeout
%% should occur at or after `TimeoutTime'. The time must be greater than or
%% equal to `Time'. If a queue does not require a timeout then `TimeoutTime'
%% should be `infinity'. The value may be ignored or unavailable in other
%% callbacks if the queue is empty.
%%
%% When inserting a request into the queue, `handle_in/6':
%% ```
%% -callback handle_in(SendTime :: integer(),
%%                     From :: {Sender :: pid(), Tag :: any()}, Value :: any(),
%%                     Time :: integer(), State :: any()) ->
%%     {NState :: any(), TimeoutTime :: integer() | infinity}.
%% '''
%% The variables are equivalent to those in `init/3', with `NState' being the
%% new state.
%%
%% When removing a request from the queue, `handle_out/2':
%% ```
%% -callback handle_out(Time :: integer(), State :: any()) ->
%%     {SendTime :: integer(), From :: {Sender :: pid(), Tag :: any()},
%%      Value:: any(), Ref :: reference, NState :: any(),
%%      TimeoutTime :: integer() | infinity} |
%%     {empty, NState :: any()}.
%% '''
%%
%% The variables are equivalent to those in `init/3', with `NState' being the
%% new state. This callback either returns a single request, added in the
%% `InternalQueue' from `init/3' or enqueued with `handle_in/6'. If the queue is
%% empty an `empty' tuple is returned.
%%
%% When a timeout occurs, `handle_timeout/2':
%% ```
%% -callback handle_timeout(Time :: integer(), State :: any()) ->
%%     {NState :: any(), TimeoutTime :: integer() | infinity}.
%% '''
%% The variables are equivalent to those in `init/3', with `NState' being the
%% new state.
%%
%% When cancelling requests, `handle_cancel/3':
%% ```
%% -callback handle_cancel(Tag :: any(), Time :: integer(), State :: any()) ->
%%     {Reply :: false | pos_integer(), NState :: any(),
%%      TimeoutTime :: integer() | infinity}.
%% '''
%% `Tag' is a response tag, which is part of the `From' tuple passed via
%% `InternalQueue' in `init/3' or directly in `handle_in/6'. There may be
%% multiple requests with the same tag and all should be removed.
%%
%% If no requests are cancelled the `Reply' is `false', otherwise it is the
%% number of cancelled requests.
%%
%% The other variables are equivalent to those in `init/3', with `NState' being
%% the new state.
%%
%% When handling a message, `handle_info/3':
%% ```
%% -callback handle_info(Msg :: any(), Time :: integer(), State :: any()) ->
%%     {NState :: any(), TimeoutTime :: integer() | infinity}.
%% '''
%% `Msg' is the message, and may be intended for another queue.
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
%% The variables are equivalent to those in `init/3', with `NState' being the
%% new state.
%%
%% When returning the number of queued requests, `len/1':
%% ```
%% -callback len(State :: any()) -> Len :: non_neg_integer().
%% '''
%% `State' is the current state of the queue and `Len' is the number of queued
%% requests. This callback must be idempotent and so not drop any requests.
%%
%% When returning the send time of the oldest request in the queue,
%% `send_time/1':
%% ```
%% -callback send_time(State :: any()) -> SendTime :: integer() | empty.
%% '''
%% `State' is the current state of the queue and `SendTime' is the send time of
%% the oldest request, if not requests then `empty'. This callback must be
%% idempotent and so not drop any requests.
%%
%% When cleaning up the queue, `terminate/2':
%% ```
%% -callback terminate(Reason :: sbroker_handlers:reason(), State :: any()) ->
%%      InternalQueue :: internal_queue().
%% '''
%% `Reason' is `stop' if the queue is being shutdown, `change' if the queue is
%% being replaced by another queue, `{bad_return_value, Return}' if a previous
%% callback returned an invalid term or `{Class, Reason, Stack}' if a previous
%% callback raised an exception.
%%
%% `State' is the current state of the queue.
%%
%% `InternalQueue' is the same as `init/3' and is passed to the next queue if
%% `Reason' is `change'.
%%
%% The process controlling the queue may not be terminating with the queue and
%% so `terminate/2' should do any clean up required.
-module(sbroker_queue).

-behaviour(sbroker_handlers).

%% public api

-export([drop/3]).

%% sbroker_handlers api

-export([initial_state/0]).
-export([init/5]).
-export([code_change/6]).
-export([config_change/5]).
-export([terminate/3]).

%% types

-type internal_queue() ::
    queue:queue({integer(), {pid(), any()}, any(), reference()}).

-export_type([internal_queue/0]).

-callback init(Q :: internal_queue(), Time :: integer(), Args :: any()) ->
    {State :: any(), TimeoutTime :: integer() | infinity}.

-callback handle_in(SendTime :: integer(),
                    From :: {Sender :: pid(), Tag :: any()}, Value :: any(),
                    Time :: integer(), State :: any()) ->
    {NState :: any(), TimeoutTime :: integer() | infinity}.

-callback handle_out(Time :: integer(), State :: any()) ->
    {SendTime :: integer(), From :: {pid(), Tag :: any()}, Value :: any(),
     Ref :: reference(), NState :: any(), TimeoutTime :: integer() | infinity} |
    {empty, NState :: any()}.

-callback handle_timeout(Time :: integer(), State :: any()) ->
    {NState :: any(), TimeoutTime :: integer() | infinity}.

-callback handle_cancel(Tag :: any(), Time :: integer(), State :: any()) ->
    {Reply :: false | pos_integer(), NState :: any(),
     TimeoutTime :: integer() | infinity}.

-callback handle_info(Msg :: any(), Time :: integer(), State :: any()) ->
    {NState :: any(), TimeoutTime :: integer() | infinity}.

-callback code_change(OldVsn :: any(), Time :: integer(), State :: any(),
                      Extra :: any()) ->
    {NState :: any(), TimeoutTime :: integer() | infinity}.

-callback config_change(Args :: any(), Time :: integer(), State :: any()) ->
    {NState :: any(), TimeoutTime :: integer() | infinity}.

-callback len(State :: any()) -> Len :: non_neg_integer().

-callback send_time(State :: any()) -> SendTime :: integer() | empty.

-callback terminate(Reason :: sbroker_handlers:reason(), State :: any()) ->
    Q :: internal_queue().

%% public api

%% @doc Drop a request from `From', sent at `SendTime' from the queue.
%%
%% Call `drop/3' when dropping a request from a queue.
-spec drop(From, SendTime, Time) -> ok when
      From :: {pid(), Tag :: any()},
      SendTime :: integer(),
      Time :: integer().
drop(From, SendTime, Time) ->
   _ = gen:reply(From, {drop, Time-SendTime}),
   ok.

%% sbroker_handlers api

%% @private
-spec initial_state() -> Q when
      Q :: internal_queue().
initial_state() ->
    queue:new().

%% @private
-spec init(Module, Q, Send, Time, Args) -> {State, TimeoutTime} when
    Module :: module(),
    Q :: internal_queue(),
    Send :: integer(),
    Time :: integer(),
    Args :: any(),
    State :: any(),
    TimeoutTime :: integer() | infinity.
init(Mod, Q, _, Now, Args) ->
    Mod:init(Q, Now, Args).

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
-spec config_change(Module, Args, Send, Time, State) ->
    {NState, TimeoutTime} when
    Module :: module(),
    Args :: any(),
    Send :: integer(),
    Time :: integer(),
    State :: any(),
    NState :: any(),
    TimeoutTime :: integer() | infinity.
config_change(Mod, Args, _, Now, State) ->
    Mod:config_change(Args, Now, State).

%% @private
-spec terminate(Module, Reason, State) -> Q when
      Module :: module(),
      Reason :: sbroker_handlers:reason(),
      State :: any(),
      Q :: internal_queue().
terminate(Mod, Reason, State) ->
    Q = Mod:terminate(Reason, State),
    case queue:is_queue(Q) of
        true -> Q;
        false -> exit({bad_return_value, Q})
    end.
