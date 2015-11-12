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
%% `init/3', which starts the queue:
%% ```
%% -callback init(TimeUnit :: sbroker_time:unit(0, Time :: integer(),
%%                Args :: any()) -> State :: any().
%% '''
%% `Time' is the time, in `TimeUnit' time units, of the queue at creation. Some
%% other callbacks will receive the current time of the queue as the second last
%% argument. It is monotically increasing, so subsequent calls will have the
%% same or a greater time.
%%
%% `Args' is the arguments for the queue. It can be any term.
%%
%% `State' is the state of the queue and used in the next call.
%%
%% When inserting a request into the queue, `handle_in/5':
%% ```
%% -callback handle_in(SendTime :: integer(),
%%                     From :: {Sender :: pid(), Tag :: any()}, Value :: any(),
%%                     Time :: integer(), State :: any()) ->
%%     {NState :: any(), TimeoutTime :: integer() | infinity}.
%% '''
%% `SendTime' is the (approximate) time a request was sent. It is always less
%% than or equal to `Time'.
%%
%% `From' is a 2-tuple containing the senders pid and a response tag. `From' can
%% be used with `drop/3' to drop a request.
%%
%% `Value' is any term, `Time' is the current time, `State' is the current
%% state and `NState'.
%%
%% `TimeoutTime' represents the next time a queue wishes to call
%% `handle_timeout/2' to drop items. If a message is not received the timeout
%% should occur at or after `TimeoutTime'. The time must be greater than or
%% equal to `Time'. If a queue does not require a timeout then `TimeoutTime'
%% should be `infinity'. The value may be ignored or unavailable in other
%% callbacks if the queue is empty.
%%
%% When removing a request from the queue, `handle_out/2':
%% ```
%% -callback handle_out(Time :: integer(), State :: any()) ->
%%     {SendTime :: integer(), From :: {Sender :: pid(), Tag :: any()},
%%      Value:: any(), NState :: any(), TimeoutTime :: integer() | infinity} |
%%     {empty, NState :: any()}.
%% '''
%%
%% `Time' is the current time, `State' is the current state and `NState' is the
%% new state.
%%
%% `SendTime', `From' and `Value' should be the same values passed as
%% arguments to `handle_in/5'.
%%
%% `TimeoutTime' is the time of the next timeout, see `handle_in/5'.
%%
%% When a timeout occurs, `handle_timeout/2':
%% ```
%% -callback handle_timeout(Time :: integer(), State :: any()) ->
%%     {NState :: any(), TimeoutTime :: integer() | infinity}.
%% '''
%% `Time' is the current time, `State' is the current state, `NState' is the
%% new state and `TimeoutTime' is the time of the next timeout, see
%% `handle_in/5'.
%%
%% When cancelling requests, `handle_cancel/3':
%% ```
%% -callback handle_cancel(Tag :: any(), Time :: integer(), State :: any()) ->
%%     {Reply :: false | pos_integer(), NState :: any(),
%%      TimeoutTime :: integer() | infinity}.
%% '''
%% `Tag' is a response tag, which is part of the `From' tuple passed to
%% `handle_in/5'. There may be multiple requests with the same tag and all
%% should be removed.
%%
%% If no requests are cancelled the `Reply' is `false', otherwise it is the
%% number of cancelled requests.
%%
%% `Time' is the current time, `State' is the current state, `NState' is the
%% new state and `TimeoutTime' is the time of the next timeout, see
%% `handle_in/5'.
%%
%% When handling a message, `handle_info/3':
%% ```
%% -callback handle_info(Msg :: any(), Time :: integer(), State :: any()) ->
%%     {NState :: any(), TimeoutTime :: integer() | infinity}.
%% '''
%% `Msg' is the message, and may be intended for another queue.
%%
%% `Time' is the current time, `State' is the current state, `NState' is the
%% new state and `TimeoutTime' is the time of the next timeout, see
%% `handle_in/5'.
%%
%% When changing the configuration of a queue, `config_change/4':
%% ```
%% -callback config_change(TimeUnit :: sbroker_time:unit(), Args :: any(),
%%                         Time :: integer(), State :: any()) ->
%%      {NState :: any(), TimeoutTime :: integer() | infinity}.
%% '''
%% `Args' is the arguments to reconfigure the queue. The queue should change its
%% configuration as if the same `Args' term was used in `init/3'.
%%
%% `Time' is the current time in `TimeUnit' time units, `State' is the current
%% state, `NState' is the new state and `TimeoutTime' is the time of the next
%% timeout, see `handle_in/5'.
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
%% `Reason' is `stop' if the queue is being shutdown, `change' if the queue is
%% being replaced by another queue, `{bad_return_value, Return}' if a previous
%% callback returned an invalid term or `{Class, Reason, Stack}' if a previous
%% callback raised an exception.
%%
%% `State' is the current state of the queue.
%%
%% The process controlling the queue may not be terminating with the queue and
%% so `terminate/2' should do any clean up required.
%% @private
-module(sbroker_queue).

-behaviour(sbroker_handlers).

%% public api

-export([drop/3]).
-export([change/7]).

%% types

-callback init(TimeUnit :: sbroker_time:unit(), Time :: integer(),
               Args :: any()) -> State :: any().

-callback handle_in(SendTime :: integer(),
                    From :: {Sender :: pid(), Tag :: any()}, Value :: any(),
                    Time :: integer(), State :: any()) ->
    {NState :: any(), TimeoutTime :: integer() | infinity}.

-callback handle_out(Time :: integer(), State :: any()) ->
    {SendTime :: integer(), From :: {pid(), Tag :: any()}, Value :: any(),
     NState :: any(), TimeoutTime :: integer() | infinity} |
    {empty, NState :: any()}.

-callback handle_timeout(Time :: integer(), State :: any()) ->
    {NState :: any(), TimeoutTime :: integer() | infinity}.

-callback handle_cancel(Tag :: any(), Time :: integer(), State :: any()) ->
    {Reply :: false | pos_integer(), NState :: any(),
     TimeoutTime :: integer() | infinity}.

-callback handle_info(Msg :: any(), Time :: integer(), State :: any()) ->
    {NState :: any(), TimeoutTime :: integer() | infinity}.

-callback config_change(TimeUnit :: sbroker_time:unit(), Args :: any(),
                        Time :: integer(), State :: any()) ->
    {NState :: any(), TimeoutTime :: integer() | infinity}.

-callback to_list(State :: any()) ->
    [{SendTime :: integer(), From :: {Sender :: pid(), Tag :: any()},
      Value :: any()}].

-callback len(State :: any()) -> Len :: non_neg_integer().

-callback terminate(Reason :: sbroker_handler:reason(), State :: any()) ->
    any().

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

%% @private
-spec change(Module1, State1, Module2, TimeUnit, Args, Time, Name) ->
    {ok, State2, TimeoutTime} | {stop, Reason, Callbacks} when
      Module1 :: module(),
      State1 :: any(),
      Module2 :: module(),
      TimeUnit :: sbroker_time:unit(),
      Args :: any(),
      Time :: integer(),
      Name :: sbroker_handler:name(),
      State2 :: any(),
      TimeoutTime :: integer(),
      Reason :: sbroker_handlers:reason(),
      Callbacks :: [{Module1 | Module2, Reason | stop, State :: any()}].
change(Mod, State, Mod, TimeUnit, Args, Now, _) ->
    try Mod:config_change(TimeUnit, Args, Now, State) of
        {NState, Next} ->
            {ok, NState, Next};
        Other ->
            Reason = {bad_return_value, Other},
            {stop, Reason, [{Mod, Reason, State}]}
    catch
        Class:Reason ->
            Reason2 = {Class, Reason, erlang:get_stacktrace()},
            {stop, Reason2, [{Mod, Reason2, State}]}
    end;
change(Mod1, State1, Mod2, TimeUnit, Args2, Now, Name) ->
    try Mod1:to_list(State1) of
        Items when is_list(Items) ->
            change_init(Items, Mod1, State1, Mod2, TimeUnit, Args2, Now, Name);
        Other ->
            Reason = {bad_return_value, Other},
            {stop, Reason, [{Mod1, Reason, State1}]}
    catch
        Class:Reason ->
            Reason2 = {Class, Reason, erlang:get_stacktrace()},
            {stop, Reason2, [{Mod1, stop, State1}]}
    end.

%% Internal

change_init(Items, Mod1, State1, Mod2, TimeUnit, Args2, Now, Name) ->
    try Mod2:init(TimeUnit, Now, Args2) of
        State2 ->
            change_from_list(Items, Mod1, State1, Mod2, State2, Now, Name)
    catch
        Class:Reason ->
            Reason2 = {Class, Reason, erlang:get_stacktrace()},
            sbroker_handler:report(?MODULE, start_error, Name, Mod2, Reason2,
                                   Args2),
            {stop, Reason2, [{Mod1, stop, State1}]}
    end.

change_from_list(Items, Mod1, State1, Mod2, State2, Now, Name) ->
    case change_in(Items, Mod2, State2, infinity, Now) of
        {ok, NState2, Next} ->
            change_terminate(Mod1, State1, Mod2, NState2, Next, Name);
        {stop, Reason, Callbacks} ->
            {stop, Reason, [{Mod1, stop, State1} | Callbacks]};
        {bad_items, Callbacks} ->
            Reason = {bad_return_value, Items},
            {stop, Reason, [{Mod1, Reason, State1} | Callbacks]}
    end.

change_in([], _, State, Next, _) ->
    {ok, State, Next};
change_in([{Send, From, Value} | Items], Mod, State, _, Now) ->
    try Mod:handle_in(Send, From, Value, Now, State) of
        {NState, Next} ->
            change_in(Items, Mod, NState, Next, Now);
        Other ->
            Reason = {bad_return_value, Other},
            {stop, Reason, [{Mod, Reason, State}]}
    catch
        Class:Reason ->
            Reason2 = {Class, Reason, erlang:get_stacktrace()},
            {stop, Reason2, [{Mod, Reason2, State}]}
    end;
change_in(_, Mod, State, _, _) ->
    {bad_items, [{Mod, stop, State}]}.

change_terminate(Mod1, State1, Mod2, State2, Next, Name) ->
    try Mod1:terminate(change, State1) of
        _ ->
            {ok, State2, Next}
    catch
        Class:Reason ->
            Reason2 = {Class, Reason, erlang:get_stacktrace()},
            sbroker_handler:report(?MODULE, handler_crashed, Mod1, Reason2,
                                   State1, Name),
            {stop, Reason2, [{Mod2, stop, State2}]}
    end.
