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
%% @doc Behaviour for implement queues for `sbroker'.
%%
%% A custom queue must implement the `sbroker' behaviour. The first callback is
%% `init/2', which starts the queue:
%% ```
%% -callback init(Time :: integer(), Args :: any()) -> State :: any().
%% '''
%% `Time' is the time of the queue at creation. Some other callbacks will
%% receive the current time of the queue as the second last argument. It is
%% monotically increasing, so subsequent calls will have the same or a greater
%% time.
%%
%% `Args' is the arguments for the queue. It can be any term.
%%
%% `State' is the state of the queue and used in the next call.
%%
%% When inserting a request into the queue, `handle_in/4':
%% ```
%% -callback handle_in(SendTime :: integer(),
%%                     From :: {Sender :: pid(), Tag :: any()},
%%                     Time :: integer(), State :: any()) ->
%%     NState :: any().
%% '''
%% `SendTime' is the (approximate) time a request was sent. It is always less
%% than or equal to `Time'.
%%
%% `From' is a 2-tuple containing the senders pid and a response tag. `From' can
%% be used with `drop/3' to drop a request.
%%
%% `Time' is the current time, `State' is the current state and `NState' is the
%% new state.
%%
%% When removing a request from the queue, `handle_out/2':
%% ```
%% -callback handle_out(Time :: integer(), State :: any()) ->
%%     {SendTime :: integer(), From :: {pid(), Tag :: any()}, NState :: any()} |
%%     {empty, NState :: any()}.
%% '''
%%
%% `Time' is the current time, `State' is the current state and `NState' is the
%% new state.
%%
%% When a timeout occurs, `handle_timeout/2':
%% ```
%% -callback handle_timeout(Time :: integer(), State :: any()) ->
%%     NState :: any().
%% '''
%% `Time' is the current time, `State' is the current state and `NState' is the
%% new state.
%%
%% When cancelling requests, `handle_cancel/3':
%% ```
%% -callback handle_cancel(Tag :: any(), Time :: integer(), State :: any()) ->
%%     {Reply :: false | pos_integer(), NState :: any()}.
%% '''
%% `Tag' is a response tag, which is part of the `From' tuple passed to
%% `handle_in/4'. There may be multiple requests with the same tag and all
%% should be removed.
%%
%% If no requests are cancelled the `Reply' is `false', otherwise it is the
%% number of cancelled requests.
%%
%% `Time' is the current time, `State' is the current state and `NState' is the
%% new state.
%%
%% When handling a message, `handle_info/3':
%% ```
%% -callback handle_info(Msg :: any(), Time :: integer(), State :: any()) ->
%%     NState :: any().
%% '''
%% `Msg' is the message, and may be intended for another queue.
%%
%% `Time' is the current time, `State' is the current state and `NState' is the
%% new state.
%%
%% When changing the configuration of a queue, `config_change/3':
%% ```
%% -callback config_change(Args :: any(), Time :: integer(), State :: any()) ->
%%      NState :: any().
%% '''
%% `Args' is the arguments to reconfigure the queue. The queue should change its
%% configuration as if the same `Args' term was used in `init/2'.
%%
%% `Time' is the current time, `State' is the current state and `NState' is the
%% new state.
%%
%% When returning a list of queued requests, `to_list/1':
%% ```
%% -callback to_list(State :: any()) ->
%%     [{SendTime :: integer(), From :: {Sender :: pid(), Tag :: any()}}].
%% '''
%% `State' is the current state of the queue, `SendTime' and `From' are the
%% values from requests inserted into the queue. The list should be ordered so
%% that the first request is at the head and last added request is at the tail.
%% This means that `SendTime' should increase from the head to the tail of the
%% list. This callback must be idempotent and so not drop any requests,
%%
%% When returning the number of queued requests, `len/1':
%% ```
%% -callback len(State :: any()) -> Len :: non_neg_integer().
%% '''
%% `State' is the current state of the queue and `Len' is the number of queued
%% requests. This callback must be idempotent and so not drop any requests.
%%
%% When cleaning up the queue, `terminate/2':
%% ```
%% -callback terminate(Reason :: stop | {bad_return_value, Return :: any()} |
%%                     {error | throw | exit, Reason :: any(), Stack :: list()},
%%                     State :: any()) -> any().
%% '''
%% `Reason' is `stop' if the queue is being shutdown,
%% `{bad_return_value, Return}' if a previous callback returned an invalid term
%% or `{Class, Reason, Stack}' if a previous callback raised an exception.
%%
%% `State' is the current state of the queue.
%%
%% The process controlling the queue may not be terminating with the queue and
%% so `terminate/2' should do any clean up required.
%% @private
-module(sbroker_queue).

%% public api

-export([drop/3]).

%% types

-callback init(Time :: integer(), Args :: any()) ->
    State :: any().

-callback handle_in(SendTime :: integer(),
                    From :: {Sender :: pid(), Tag :: any()}, Time :: integer(),
                    State :: any()) -> NState :: any().

-callback handle_out(Time :: integer(), State :: any()) ->
    {SendTime :: integer(), From :: {pid(), Tag :: any()}, NState :: any()} |
    {empty, NState :: any()}.

-callback handle_timeout(Time :: integer(), State :: any()) -> NState :: any().

-callback handle_cancel(Tag :: any(), Time :: integer(), State :: any()) ->
    {Reply :: false | pos_integer(), NState :: any()}.

-callback handle_info(Msg :: any(), Time :: integer(), State :: any()) ->
    NState :: any().

-callback config_change(Args :: any(), Time :: integer(), State :: any()) ->
    NState :: any().

-callback to_list(State :: any()) ->
    [{SendTime :: integer(), From :: {Sender :: pid(), Tag :: any()}}].

-callback len(State :: any()) -> Len :: non_neg_integer().

-callback terminate(Reason :: stop | {bad_return_value, Return :: any()} |
                    {error | throw | exit, Reason :: any(), Stack :: list()},
                    State :: any()) -> any().

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
