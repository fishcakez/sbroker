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
-module(sbroker_meter_statem).

-include_lib("proper/include/proper.hrl").

-compile({no_auto_import, [time/0]}).

-export([quickcheck/0]).
-export([quickcheck/1]).
-export([check/1]).
-export([check/2]).

-export([initial_state/0]).
-export([command/1]).
-export([precondition/2]).
-export([next_state/3]).
-export([postcondition/3]).

-export([init_or_change/6]).
-export([handle_update/6]).

-record(state, {manager, manager_state, mod, meter, time, timeout_time}).

quickcheck() ->
    quickcheck([]).

quickcheck(Opts) ->
    proper:quickcheck(prop_sbroker_meter(), Opts).

check(CounterExample) ->
    check(CounterExample, []).

check(CounterExample, Opts) ->
    proper:check(prop_sbroker_meter(), CounterExample, Opts).

prop_sbroker_meter() ->
    ?FORALL(Cmds, commands(?MODULE),
            ?TRAPEXIT(begin
                          {History, State, Result} = run_commands(?MODULE, Cmds),
                          sbroker_test_handler:reset(),
                          ?WHENFAIL(begin
                                        io:format("History~n~p", [History]),
                                        io:format("State~n~p", [State]),
                                        io:format("Result~n~p", [Result])
                                    end,
                                    aggregate(command_names(Cmds), Result =:= ok))
                      end)).

initial_state() ->
    #state{}.

command(#state{mod=undefined} = State) ->
    {call, ?MODULE, init_or_change, init_or_change_args(State)};
command(#state{mod=Mod} = State) ->
    frequency([{16, {call, ?MODULE, handle_update, handle_update_args(State)}},
               {4, {call, ?MODULE, init_or_change, init_or_change_args(State)}},
               {1, {call, Mod, handle_info, handle_info_args(State)}},
               {1, {call, Mod, terminate, terminate_args(State)}}]).

precondition(State, {call, _, init_or_change, Args}) ->
    init_or_change_pre(State, Args);
precondition(#state{manager=undefined}, _) ->
    false;
precondition(State, {call, _, handle_update, Args}) ->
    handle_update_pre(State, Args);
precondition(State, {call, _, handle_info, Args}) ->
    handle_info_pre(State, Args);
precondition(State, {call, _, terminate, Args}) ->
    terminate_pre(State, Args).

next_state(State, Value, {call, _, init_or_change, Args}) ->
    init_or_change_next(State, Value, Args);
next_state(State, Value, {call, _, handle_update, Args}) ->
    handle_update_next(State, Value, Args);
next_state(State, Value, {call, _, handle_info, Args}) ->
    handle_info_next(State, Value, Args);
next_state(State, Value, {call, _, terminate, Args}) ->
    terminate_next(State, Value, Args).

postcondition(State, {call, _, init_or_change, Args}, Result) ->
    init_or_change_post(State, Args, Result);
postcondition(State, {call, _, handle_update, Args}, Result) ->
    handle_update_post(State, Args, Result);
postcondition(State, {call, _, handle_info, Args}, Result) ->
    handle_info_post(State, Args, Result);
postcondition(State, {call, _, terminate, Args}, Result) ->
    terminate_post(State, Args, Result).

manager() ->
    frequency([{4, sbroker_alarm_statem},
               {1, sbroker_timeout_meter_statem}]).

time() ->
    ?LET(Time, choose(-10, 10),
         sbroker_time:convert_time_unit(Time, milli_seconds, native)).

time(undefined) ->
    time();
time(Time) ->
    oneof([Time,
           ?LET(Incr, choose(5, 5),
                sbroker_time:convert_time_unit(Time + Incr, milli_seconds,
                                               native))]).

init_or_change(undefined, undefined, _, Mod, Args, Time) ->
    Mod:init(Time, Args);
init_or_change(Mod1, State1, _, Mod2, Args2, Time) ->
    sbroker_meter:change(Mod1, State1, Mod2, Args2, Time, ?MODULE).

init_or_change_args(#state{mod=Mod, meter=M, time=Time}) ->
    ?LET(Manager, manager(),
         [Mod, M, Manager, Manager:module(), Manager:args(), time(Time)]).

init_or_change_pre(#state{manager=undefined, mod=undefined}, _) ->
    true;
init_or_change_pre(#state{time=PrevTime}, [_, _, _, _, _, Time]) ->
    PrevTime >= Time.

init_or_change_next(#state{manager=undefined} = State, M,
                    [_, _, Manager, Mod, Args, Time]) ->
    ManState = Manager:init(Time, Args),
    State#state{manager=Manager, mod=Mod, meter=M, time=Time, timeout_time=Time,
                manager_state=ManState};
init_or_change_next(#state{manager=Manager, manager_state=ManState} = State,
                   Value, [Mod, _, Manager, Mod, Args, Time]) ->
    M = {call, erlang, element, [2, Value]},
    {NManState, Timeout} = Manager:change(ManState, Time, Args),
    State#state{meter=M, manager_state=NManState, time=Time,
                timeout_time=Timeout};
init_or_change_next(_, Value, [_, _, Manager, Mod, Args, Time]) ->
    M = {call, erlang, element, [2, Value]},
    State = init_or_change_next(initial_state(), M, [undefined, undefined,
                                                     Manager, Mod, Args, Time]),
    State#state{time=Time}.

init_or_change_post(#state{manager=undefined}, _, _) ->
    true;
init_or_change_post(#state{manager=Manager, manager_state=ManState},
                    [Mod, _, Manager, Mod, Args, Time], {ok, _, Timeout}) ->
    {_, Timeout2} = Manager:change(ManState, Time, Args),
    timeout_post(Timeout2, Timeout);
init_or_change_post(State, [_, M, _, _, _, _], {ok, _, Timeout}) ->
    Timeout =:= infinity andalso terminate_post(State, undefined, [change, M]).

handle_update(Mod, MsgQLen, ProcessTime, Count, Time, M) ->
    Refs = [self() ! make_ref() || _ <- lists:seq(1, MsgQLen)],
    Result = Mod:handle_update(ProcessTime, Count, Time, M),
    _ = [receive Ref -> ok after 0 -> exit(timeout) end || Ref <- Refs],
    Result.

handle_update_args(#state{mod=Mod, time=Time, meter=M}) ->
    [Mod, choose(0, 3), choose(0, 5), choose(0, 5), time(Time), M].

handle_update_pre(#state{time=PrevTime}, [_, _, _, _, Time, _]) ->
    Time >= PrevTime.

handle_update_next(#state{manager=Manager, manager_state=ManState} = State,
                   Value, [_, MsgQLen, QueueDelay, ProcessDelay, Time, _]) ->
    M = {call, erlang, element, [1, Value]},
    {NManState, Timeout} = Manager:update_next(ManState, Time, MsgQLen,
                                               QueueDelay, ProcessDelay),
    State#state{meter=M, time=Time, timeout_time=Timeout,
                manager_state=NManState}.

handle_update_post(#state{manager=Manager, manager_state=ManState},
                   [_, MsgQLen, QueueDelay, ProcessDelay, Time, _],
                   {_, Timeout}) ->
    {Result, Timeout2} = Manager:update_post(ManState, Time, MsgQLen,
                                             QueueDelay, ProcessDelay),
    Result andalso timeout_post(Timeout2, Timeout).

handle_info_args(#state{time=Time, meter=M}) ->
    [oneof([a, b, c]), time(Time), M].

handle_info_pre(#state{time=PrevTime}, [_, Time, _]) ->
    Time >= PrevTime.

handle_info_next(State, Value, [_, Time, _]) ->
    M = {call, erlang, element, [1, Value]},
    State#state{meter=M, time=Time}.

handle_info_post(#state{manager=Manager, manager_state=ManState},
                   [_, Time, _], {_, Timeout}) ->
    Timeout2 = Manager:timeout(ManState, Time),
    timeout_post(Timeout2, Timeout).

terminate_args(#state{meter=M}) ->
    [oneof([shutdown, normal, abnormal]), M].

terminate_pre(_, _) ->
    true.

terminate_next(_, _, _) ->
    initial_state().

terminate_post(_, _, _) ->
    true.

%% Helpers

timeout_post(Expected, Observed) when Expected =:= Observed ->
    true;
timeout_post(Expected, Observed) ->
    ct:pal("Timeout~nExpected: ~p~nObserved: ~p", [Expected, Observed]),
    false.
