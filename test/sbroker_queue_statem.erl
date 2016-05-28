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
-module(sbroker_queue_statem).

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
-export([in/6]).
-export([handle_out_queue/1]).
-export([handle_out_timeout/1]).
-export([shutdown/5]).

-record(state, {manager, manager_state, mod, queue, list=[], outs=[],
                cancels=[], drops=[], out, drop, min, max, time, send_time,
                timeout_time}).

quickcheck() ->
    quickcheck([]).

quickcheck(Opts) ->
    proper:quickcheck(prop_sbroker_queue(), Opts).

check(CounterExample) ->
    check(CounterExample, []).

check(CounterExample, Opts) ->
    proper:check(prop_sbroker_queue(), CounterExample, Opts).

prop_sbroker_queue() ->
    ?FORALL(Cmds, commands(?MODULE),
            ?TRAPEXIT(begin
                          {History, State, Result} = run_commands(?MODULE, Cmds),
                          ok,
                          ?WHENFAIL(begin
                                        io:format("History~n~p", [History]),
                                        io:format("State~n~p", [State]),
                                        io:format("Result~n~p", [Result])
                                    end,
                                    aggregate(command_names(Cmds), Result =:= ok))
                      end)).

initial_state() ->
    #state{}.

command(#state{manager=undefined} = State) ->
    {call, ?MODULE, init_or_change, init_or_change_args(State)};
command(#state{mod=Mod} = State) ->
    frequency([{20, {call, ?MODULE, in, in_args(State)}},
               {10, {call, Mod, handle_out, out_args(State)}},
               {4, {call, ?MODULE, init_or_change, init_or_change_args(State)}},
               {4, {call, Mod, handle_timeout, timeout_args(State)}},
               {4, {call, Mod, handle_cancel, cancel_args(State)}},
               {4, {call, Mod, handle_info, info_args(State)}},
               {4, {call, ?MODULE, shutdown, shutdown_args(State)}},
               {4, {call, Mod, len, len_args(State)}},
               {1, {call, Mod, terminate, terminate_args(State)}}]).

precondition(State, {call, _, init_or_change, Args}) ->
    init_or_change_pre(State, Args);
precondition(#state{manager=undefined}, _) ->
    false;
precondition(State, {call, _, in, Args}) ->
    in_pre(State, Args);
precondition(State, {call, _, handle_out, Args}) ->
    handle_out_pre(State, Args);
precondition(State, {call, _, handle_timeout, Args}) ->
    handle_timeout_pre(State, Args);
precondition(State, {call, _, handle_cancel, Args}) ->
    handle_cancel_pre(State, Args);
precondition(State, {call, _, handle_info, Args}) ->
    handle_info_pre(State, Args);
precondition(State, {call, _, shutdown, Args}) ->
    shutdown_pre(State, Args);
precondition(State, {call, _, len, Args}) ->
    len_pre(State, Args);
precondition(State, {call, _, terminate, Args}) ->
    terminate_pre(State, Args).

next_state(State, Value, {call, _, init_or_change, Args}) ->
    init_or_change_next(State, Value, Args);
next_state(State, Value, {call, _, in, Args}) ->
    in_next(State, Value, Args);
next_state(State, Value, {call, _, handle_out, Args}) ->
    handle_out_next(State, Value, Args);
next_state(State, Value, {call, _, handle_timeout, Args}) ->
    handle_timeout_next(State, Value, Args);
next_state(State, Value, {call, _, handle_cancel, Args}) ->
    handle_cancel_next(State, Value, Args);
next_state(State, Value, {call, _, handle_info, Args}) ->
    handle_info_next(State, Value, Args);
next_state(State, Value, {call, _, shutdown, Args}) ->
    shutdown_next(State, Value, Args);
next_state(State, Value, {call, _, len, Args}) ->
    len_next(State, Value, Args);
next_state(State, Value, {call, _, terminate, Args}) ->
    terminate_next(State, Value, Args).

postcondition(State, {call, _, init_or_change, Args}, Result) ->
    init_or_change_post(State, Args, Result);
postcondition(State, {call, _, in, Args}, Result) ->
    in_post(State, Args, Result);
postcondition(State, {call, _, handle_out, Args}, Result) ->
    handle_out_post(State, Args, Result);
postcondition(State, {call, _, handle_timeout, Args}, Result) ->
    handle_timeout_post(State, Args, Result);
postcondition(State, {call, _, handle_cancel, Args}, Result) ->
    handle_cancel_post(State, Args, Result);
postcondition(State, {call, _, handle_info, Args}, Result) ->
    handle_info_post(State, Args, Result);
postcondition(State, {call, _, shutdown, Args}, Result) ->
    shutdown_post(State, Args, Result);
postcondition(State, {call, _, len, Args}, Result) ->
    len_post(State, Args, Result);
postcondition(State, {call, _, terminate, Args}, Result) ->
    terminate_post(State, Args, Result).

manager() ->
    frequency([{1, sbroker_statem_statem},
               {2, sbroker_drop_statem},
               {4, sbroker_timeout_statem},
               {6, sbroker_fair_statem},
               {8, sbroker_codel_statem}]).

time() ->
    ?LET(Time, choose(-10, 10),
         sbroker_time:convert_time_unit(Time, milli_seconds, native)).

time(undefined) ->
    time();
time(Time) ->
    oneof([Time,
           ?LET(Incr, choose(5, 5),
                Time + sbroker_time:convert_time_unit(Incr, milli_seconds,
                                                      native))]).

tag() ->
    elements([a, b, c]).

init_or_change(undefined, undefined, _, Mod, Args, Time) ->
    {State, Timeout} = Mod:init(queue:new(), Time, Args),
    {ok, State, Timeout};
init_or_change(Mod1, State1, _, Mod2, Args2, Time) ->
    Callback = {sbroker_queue, Mod1, State1, Mod2, Args2},
    Name = {?MODULE, self()},
    case sbroker_handlers:config_change(Time, [Callback], [], [], Name) of
        {ok, [{_, _, NState, Timeout}], {[], infinity}} ->
            {ok, NState, Timeout};
        {stop, _} = Stop ->
            Stop
    end.

init_or_change_args(#state{mod=Mod, queue=Q, time=Time}) ->
    ?LET(Manager, manager(),
         [Mod, Q, Manager, Manager:module(), Manager:args(), time(Time)]).

init_or_change_pre(#state{manager=undefined, mod=undefined}, _) ->
    true;
init_or_change_pre(#state{time=PrevTime}, [_, _, _, _, _, Time]) ->
    PrevTime =< Time.

init_or_change_next(#state{manager=undefined} = State, Value,
                    [_, _, Manager, Mod, Args, Time]) ->
    Q = {call, erlang, element, [2, Value]},
    Timeout = {call, erlang, element, [3, Value]},
    {Out, Drop, Min, Max, ManState} = Manager:init(Args),
    State#state{manager=Manager, mod=Mod, queue=Q, time=Time, send_time=Time,
                timeout_time=Timeout, manager_state=ManState, out=Out,
                drop=Drop, min=Min, max=Max};
init_or_change_next(#state{manager=Manager, manager_state=ManState} = State,
                   Value, [Mod, _, Manager, Mod, Args, Time]) ->
    Q = {call, erlang, element, [2, Value]},
    Timeout = {call, erlang, element, [3, Value]},
    {Out, Drop, Min, Max, NManState} = Manager:config_change(Time, Args,
                                                             ManState),
    NState = State#state{manager_state=NManState, out=Out, drop=Drop, min=Min,
                         max=Max},
    change_next(NState, Time, Q, Timeout);
init_or_change_next(#state{list=L, time=PrevTime, send_time=SendTime}, Value,
                    [_, _, Manager, Mod, Args, Time]) ->
    Q = {call, erlang, element, [2, Value]},
    Timeout = {call, erlang, element, [3, Value]},
    State = init_or_change_next(initial_state(), Q, [undefined, undefined,
                                                     Manager, Mod, Args, Time]),
    NState = State#state{list=L, time=PrevTime, send_time=SendTime},
    change_next(NState, Time, Q, Timeout).

change_next(State, Time, Q, Timeout) ->
    Next = fun(#state{list=L, max=Max, drop=drop, drops=Drops} = NState)
                 when length(L) > Max ->
                   {Drops2, NL} = lists:split(length(L)-Max, L),
                   NState#state{list=NL, drops=Drops++Drops2};
              (#state{list=L, max=Max, drop=drop_r, drops=Drops} = NState)
                when length(L) > Max ->
                   {NL, Drops2} = lists:split(Max, L),
                   NState#state{list=NL, drops=Drops++Drops2};
              (NState) ->
                   NState
           end,
    manager_next(State, handle_timeout, Time, Q, Timeout, Next).

init_or_change_post(#state{manager=undefined}, _, _) ->
    true;
init_or_change_post(#state{manager=Manager, manager_state=ManState} = State,
                    [Mod, _, Manager, Mod, Args, Time], {ok, Q, Timeout}) ->
    {Out, Drop, Min, Max, NManState} = Manager:config_change(Time, Args,
                                                             ManState),
    NState = State#state{manager_state=NManState, out=Out, drop=Drop, min=Min,
                         max=Max, timeout_time=Time},
    change_post(NState, Time, Q, Timeout);
init_or_change_post(#state{list=L, time=PrevTime, send_time=SendTime},
                    [_, _, Manager, Mod, Args, Time],
                    {ok, Q, Timeout} = Result) ->
    State = init_or_change_next(initial_state(), Result,
                                [undefined, undefined, Manager, Mod, Args,
                                 Time]),
    NState = State#state{list=L, time=PrevTime, send_time=SendTime},
    change_post(NState, Time, Q, Timeout).

change_post(State, Time, Q, Timeout) ->
    Post = fun(#state{list=L, max=Max, drop=drop, drops=Drops} = NState)
                 when length(L) > Max ->
                   {Drops2, NL} = lists:split(length(L)-Max, L),
                   Result = drops_post(Drops2),
                   {Result, NState#state{list=NL, drops=Drops++Drops2}};
              (#state{list=L, max=Max, drop=drop_r, drops=Drops} = NState)
                when length(L) > Max ->
                   {NL, Drops2} = lists:split(Max, L),
                   Result = drops_post(Drops2),
                   {Result, NState#state{list=NL, drops=Drops++Drops2}};
              (NState) ->
                   {true, NState}
           end,
    manager_post(State, handle_timeout, Time, Q, Timeout, Post).

info() ->
    oneof([{x}, {y}, {z}]).

in_args(#state{mod=Mod, send_time=SendTime, time=Time, queue=Q}) ->
    [Mod, oneof([SendTime, choose(SendTime, Time)]), tag(), info(), time(Time),
     Q].

in(Mod, SendTime, Tag, Info, Time, Q) ->
    Pid = spawn_client(),
    From = {Pid, Tag},
    {NQ, Timeout} = Mod:handle_in(SendTime, From, Info, Time, Q),
    {Pid, NQ, Timeout}.

in_pre(#state{mod=Mod, send_time=PrevSendTime, time=PrevTime},
       [Mod2, SendTime, _, _, Time, _]) ->
    Mod =:= Mod2 andalso SendTime =< Time andalso
    SendTime >= PrevSendTime andalso Time >= PrevTime.

in_next(State, Value,
        [_, SendTime, Tag, Info, Time, _]) ->
    Pid = {call, erlang, element, [1, Value]},
    Elem = {Time - SendTime, {Pid, Tag}, Info},
    Q = {call, erlang, element, [2, Value]},
    Timeout = {call, erlang, element, [3, Value]},
    Next = fun(#state{max=0, drops=Drops} = NState) ->
                   NState#state{drops=Drops ++ [Elem]};
              (#state{drop=drop, max=Max, list=L, drops=Drops} = NState)
                when length(L) =:= Max ->
                   NState#state{list=tl(L)++[Elem], drops=Drops ++ [hd(L)]};
              (#state{drop=drop_r, max=Max, list=L, drops=Drops} = NState)
                when length(L) =:= Max ->
                   NState#state{drops=Drops ++ [Elem]};
              (#state{list=L} = NState) ->
                   NState#state{list=L++[Elem]}
           end,
    manager_next(State#state{send_time=SendTime}, handle_timeout, Time, Q,
                 Timeout, Next).

in_post(State, [_, SendTime, Tag, Info, Time, _], {Pid, Q, Timeout}) ->
    From = {Pid, Tag},
    Sojourn = Time - SendTime,
    Elem = {Time - SendTime, From, Info},
    Post = fun(#state{max=0} = NState) ->
                   {drop_post(From, Sojourn), NState};
              (#state{drop=drop, max=Max,
                      list=[{Sojourn2, From2, _} | NL] = L} = NState)
                when length(L) =:= Max ->
                   {drop_post(From2, Sojourn2), NState#state{list=NL++[Elem]}};
              (#state{drop=drop_r, max=Max, list=L} = NState)
                when length(L) =:= Max ->
                   {drop_post(From, Sojourn), NState};
              (#state{list=L} = NState) ->
                   {true, NState#state{list=L++[Elem]}}
           end,
    manager_post(State, handle_timeout, Time, Q, Timeout, Post).

out_args(#state{time=Time, queue=Q}) ->
    [time(Time), Q].

handle_out_pre(#state{time=PrevTime}, [Time, _]) ->
    Time >= PrevTime.

handle_out_next(#state{out=out} = State, Value, [Time, _]) ->
    Q = {call, ?MODULE, handle_out_queue, [Value]},
    Timeout = {call, ?MODULE, handle_out_timeout, [Value]},
    Next = fun(#state{list=[]} = NState) ->
                   NState;
              (#state{list=[H|L], outs=Outs} = NState) ->
                   NState#state{list=L, outs=Outs++[H]}
           end,
    manager_next(State, handle_out, Time, Q, Timeout, Next);
handle_out_next(#state{out=out_r} = State, Value, [Time, _]) ->
    Q = {call, ?MODULE, handle_out_queue, [Value]},
    Timeout = {call, ?MODULE, handle_out_timeout, [Value]},
    Next = fun(#state{list=[]} = NState) ->
                   NState;
              (#state{list=L, outs=Outs} = NState) ->
                   NState#state{list=droplast(L), outs=Outs++[lists:last(L)]}
           end,
    manager_next(State, handle_out_r, Time, Q, Timeout, Next).

handle_out_queue({_, _, _, _, Q, _}) ->
    Q;
handle_out_queue({empty, Q}) ->
    Q.

handle_out_timeout({_, _, _, _, _, Timeout}) ->
    Timeout;
handle_out_timeout({empty, _}) ->
    infinity.

handle_out_post(#state{out=out} = State, [Time, _], {empty, Q}) ->
    Post = fun(#state{list=[]} = NState) ->
                   {true, NState};
              (#state{list=[_|_] = L} = NState) ->
                   ct:pal("Not empty ~p", [L]),
                   {false, NState}
           end,
    manager_post(State, handle_out, Time, Q, infinity, Post);
handle_out_post(#state{out=out} = State, [Time, _],
                {SendTime, From, Info, Ref, Q, Timeout}) ->
    Post = fun(#state{list=[{Sojourn2, From2, Info2} | NL]} = NState) ->
                   Result = (Time - SendTime =:= Sojourn2) andalso
                            From == From2 andalso Info == Info2 andalso
                            out_post(Ref),
                   {Result, NState#state{list=NL}};
              (#state{list=[]} = NState) ->
                   ct:pal("Should be empty", []),
                   {false, NState}
    end,
    manager_post(State, handle_out, Time, Q, Timeout, Post);
handle_out_post(#state{out=out_r} = State, [Time, _], {empty, Q}) ->
    Post = fun(#state{list=[]} = NState) ->
                   {true, NState};
              (#state{list=[_|_] = L} = NState) ->
                   ct:pal("Not empty ~p", [L]),
                   {false, NState}
           end,
    manager_post(State, handle_out_r, Time, Q, infinity, Post);
handle_out_post(#state{out=out_r} = State, [Time, _],
                {SendTime, From, Info, Ref, Q, Timeout}) ->
    Post = fun(#state{list=[_|_] = L} = NState) ->
                   Sojourn = Time - SendTime,
                   NL = droplast(L),
                   Result = lists:last(L) =:= {Sojourn, From, Info} andalso
                       out_post(Ref),
                   {Result, NState#state{list=NL}};
              (#state{list=[]} = NState) ->
                   ct:pal("Should be empty"),
                   {false, NState}
           end,
    manager_post(State, handle_out_r, Time, Q, Timeout, Post);
handle_out_post(_, _, _) ->
    false.

out_post(Ref) ->
    case demonitor(Ref, [flush, info]) of
        true ->
            true;
        false ->
            ct:pal("Monitor ~p does not exist", [Ref]),
            false
    end.

timeout_args(#state{time=Time, queue=Q}) ->
    [time(Time), Q].

handle_timeout_pre(#state{time=PrevTime}, [Time, _]) ->
    Time >= PrevTime.

handle_timeout_next(State, Value, [Time, _]) ->
    Q = {call, erlang, element, [1, Value]},
    Timeout = {call, erlang, element, [2, Value]},
    Next = fun(NState) -> NState end,
    manager_next(State, handle_timeout, Time, Q, Timeout, Next).

handle_timeout_post(State, [Time, _], {Q, Timeout}) ->
    Post = fun(NState) -> {true, NState} end,
    manager_post(State, handle_timeout, Time, Q, Timeout, Post).

cancel_args(#state{time=Time, queue=Q}) ->
    [tag(), time(Time), Q].

handle_cancel_pre(#state{time=PrevTime}, [_, Time, _]) ->
    Time >= PrevTime.

handle_cancel_next(State, Value, [Tag, Time,  _]) ->
    Q = {call, erlang, element, [2, Value]},
    Timeout = {call, erlang, element, [3, Value]},
    Next = fun(#state{list=L, cancels=Cancels} = NState) ->
                   {NL, Cancels2} = lists:partition(fun({_, {_, Tag2}, _}) ->
                                                            Tag =/= Tag2
                                                    end, L),
                   NState#state{list=NL, cancels=Cancels++Cancels2}
           end,
    manager_next(State, handle_timeout, Time, Q, Timeout, Next).

handle_cancel_post(State, [Tag, Time, _], {Cancelled, Q, Timeout}) ->
    Post =  fun(#state{list=L, cancels=Cancels} = NState) ->
                   {NL, Cancels2} = lists:partition(fun({_, {_, Tag2}, _}) ->
                                                            Tag =/= Tag2
                                                    end, L),
                   Result = cancel_post(Cancels2, Cancelled),
                   {Result, NState#state{list=NL, cancels=Cancels++Cancels2}}
           end,
    manager_post(State, handle_timeout, Time, Q, Timeout, Post).

cancel_post([], false) ->
    true;
cancel_post([_|_] = Cancels, Cancelled) when length(Cancels) == Cancelled ->
    true;
cancel_post([], Cancelled) ->
    ct:pal("Cancelled~nExpected: ~p~nObserved: ~p", [false, Cancelled]),
    false;
cancel_post([_|_] = Cancels, Cancelled) ->
    ct:pal("Cancelled~nExpected: ~p~nObserved: ~p", [length(Cancels),
                                                     Cancelled]),
    false.

info_args(#state{time=Time, queue=Q}) ->
    [tag(), time(Time), Q].

handle_info_pre(#state{time=PrevTime}, [_, Time, _]) ->
    Time >= PrevTime.

handle_info_next(State, Value, [_, Time, _]) ->
    Q = {call, erlang, element, [1, Value]},
    Timeout = {call, erlang, element, [2, Value]},
    Next = fun(NState) -> NState end,
    manager_next(State, handle_timeout, Time, Q, Timeout, Next).

handle_info_post(State, [_, Time, _], {Q, Timeout}) ->
    Post = fun(NState) -> {true, NState} end,
    manager_post(State, handle_timeout, Time, Q, Timeout, Post).

shutdown_args(#state{list=L, cancels=Cancels, outs=Outs, drops=Drops, mod=Mod,
                     time=Time, queue=Q}) ->
    Args = [time(Time), Q],
    NoMon = [Mod, oneof([self] ++ Cancels ++ Outs ++ Drops), nomonitor | Args],
    case L of
        [] ->
            NoMon;
        _ ->
            Mon = [Mod, oneof(L), monitor | Args],
            oneof([NoMon, Mon])
    end.

shutdown(Mod, self, nomonitor, Time, Q) ->
    Pid = self(),
    Msg = {'DOWN', make_ref(), process, Pid, shutdown},
    Mod:handle_info(Msg, Time, Q);
shutdown(Mod, {_, {Pid, _}, _}, nomonitor, Time, Q) ->
    Msg = {'DOWN', make_ref(), process, Pid, shutdown},
    Mod:handle_info(Msg, Time, Q);
shutdown(Mod, {_, {Pid, _}, _}, monitor, Time, Q) ->
    exit(Pid, shutdown),
    receive
        {'DOWN', _, process, Pid, _} = Down ->
            Mod:handle_info(Down, Time, Q)
    after
        5000 ->
            timeout
    end.

shutdown_pre(#state{time=PrevTime, list=L}, [_, Elem, monitor, Time, _]) ->
    Time >= PrevTime andalso lists:member(Elem, L);
shutdown_pre(#state{time=PrevTime, cancels=Cancels, outs=Outs, drops=Drops},
             [_, Elem, nomonitor, Time, _]) ->
    Time >= PrevTime andalso (lists:member(Elem, Cancels) orelse
    lists:member(Elem, Outs) orelse lists:member(Elem, Drops) orelse
    Elem =:= self);
shutdown_pre(_, _) ->
    true.

shutdown_next(State, Value, [_, {_, From, _}, _, Time, _]) ->
    Q = {call, erlang, element, [1, Value]},
    Timeout = {call, erlang, element, [2, Value]},
    Next = fun(#state{list=L, cancels=Cancels, outs=Outs,
                      drops=Drops} = NState) ->
                   Remove = fun({_, From2, _}) -> From2 =/= From end,
                   NL = lists:filter(Remove, L),
                   NCancels = lists:filter(Remove, Cancels),
                   NOuts = lists:filter(Remove, Outs),
                   NDrops = lists:filter(Remove, Drops),
                   NState#state{list=NL, drops=NDrops, cancels=NCancels,
                                outs=NOuts}
           end,
    manager_next(State, handle_timeout, Time, Q, Timeout, Next);
shutdown_next(State, Value, [_, self, _, Time, _]) ->
    Q = {call, erlang, element, [1, Value]},
    Timeout = {call, erlang, element, [2, Value]},
    Next = fun(NState) -> NState end,
    manager_next(State, handle_timeout, Time, Q, Timeout, Next).

shutdown_post(State, [_, {_, From, _}, _, Time, _], {Q, Timeout}) ->
    Post = fun(#state{list=L, cancels=Cancels, outs=Outs,
                      drops=Drops} = NState) ->
                   Remove = fun({_, From2, _}) -> From2 =/= From end,
                   NL = lists:filter(Remove, L),
                   NCancels = lists:filter(Remove, Cancels),
                   NOuts = lists:filter(Remove, Outs),
                   NDrops = lists:filter(Remove, Drops),
                   {true, NState#state{list=NL, drops=NDrops, cancels=NCancels,
                                       outs=NOuts}}
           end,
    manager_post(State, handle_timeout, Time, Q, Timeout, Post, From);
shutdown_post(State, [_, self, _, Time, _], {Q, Timeout}) ->
    Post = fun(NState) -> {true, NState} end,
    manager_post(State, handle_timeout, Time, Q, Timeout, Post);
shutdown_post(_, [_, {_, {Pid, _}, _}, _, _, _], timeout) ->
    ct:pal("Pid ~p DOWN timeout", [Pid]),
    false.

len_args(#state{queue=Q}) ->
    [Q].

len_pre(_, _) ->
    true.

len_next(State, _, _) ->
    State.

len_post(#state{list=L}, _, Len) ->
    case length(L) of
        Len ->
            true;
        ExpLen ->
            ct:pal("Length~nExpected: ~p~nObserved: ~p", [ExpLen, Len]),
            false
    end.

terminate_args(#state{queue=Q}) ->
    [oneof([change, stop]), Q].

terminate_pre(_, _) ->
    true.

terminate_next(State, _, _) ->
    State.

terminate_post(#state{time=Time, list=L} = State, _, Result) ->
    queue:is_queue(Result) andalso
    [{Time - SendTime, From, Info} ||
     {SendTime, From, Info, _} <- queue:to_list(Result)] =:= L andalso
    post(State).

manager_next(State, Fun, Time, Q, Timeout, Next) ->
    NState = update_time(State#state{queue=Q, timeout_time=Timeout}, Time),
    {_, NState2} = manager(manager_before(NState, Fun, Next, next), Fun, Time),
    manager_after(NState2, Fun, Next, next).

manager_post(State, Fun, Time, Q, Timeout, Post) ->
    manager_post(State, Fun, Time, Q, Timeout, Post, ignore).

manager_post(#state{manager=Manager, timeout_time=PrevTimeout} = State, Fun,
             Time, Q, Timeout, Post, Skip) ->
    NState = update_time(State#state{queue=Q, timeout_time=Timeout}, Time),
    {Result1, NState2} = manager_before(NState, Fun, Post, post),
    {Drops, NState3} = manager(NState2, Fun, Time),
    Result2 = lists:all(fun({_, From, _}) when From =:= Skip ->
                                true;
                           ({Sojourn, From, _}) ->
                                drop_post(From, Sojourn)
                        end, Drops),
    {Result3, NState4} = manager_after(NState3, Fun, Post, post),
    Result = Result1 andalso Result2 andalso Result3 andalso post(NState4),
    #state{list=L, min=Min, manager_state=ManState} = NState4,
    case Manager:time_dependence(ManState) of
        _ when Timeout =:= ignore ->
            Result;
        _ when length(L) < (Min - 1), length(Drops) > 0 ->
            ct:pal("Drops takes list below ~p to ~p", [Min-1, L]),
            false;
        dependent
          when is_integer(PrevTimeout), PrevTimeout > Time, Drops =/= [] ->
            ct:pal("No drops and previous timeout ~p>~p", [PrevTimeout, Time]),
            false;
        dependent when Timeout < Time ->
            ct:pal("Timeout ~p<~p", [Timeout, Time]),
            false;
        dependent when Timeout =:= infinity, L =/= [], length(L) > Min ->
            ct:pal("Infinity timeout but not less than min ~p", [L]),
            false;
        dependent when is_integer(Timeout), L == [],
                       Manager =/= sbroker_fair_statem ->
            ct:pal("Non-infinity timeout when empty"),
            false;
        dependent when is_integer(Timeout) andalso (length(L) =< Min),
                       Manager =/= sbroker_fair_statem ->
            ct:pal("Non-infinity timeout when less than min ~p", [L]),
            false;
        dependent when not (is_integer(Timeout) orelse Timeout =:= infinity) ->
            ct:pal("Invalid timeout ~p", [Timeout]),
            false;
        independent when Timeout =/= infinity ->
            ct:pal("Non-infinity timeout ~p", [Timeout]),
            false;
        _ ->
            Result
    end.

update_time(#state{list=L, time=PrevTime} = State, Time) ->
    NL = [{Sojourn + Time - PrevTime, From, Info} ||
          {Sojourn, From, Info} <- L],
    State#state{list=NL, time=Time}.

manager_before(State, handle_timeout, Fun, _) ->
    Fun(State);
manager_before(State, _, _, next) ->
    State;
manager_before(State, _, _, post) ->
    {true, State}.

manager(#state{manager=Manager, manager_state=ManState, list=L,
               drops=Drops} = State, Fun, Time) ->
    {Sojourns, _, _} = lists:unzip3(L),
    {DropCount, NManState} = Manager:Fun(Time, Sojourns, ManState),
    {Drops2, NL} = lists:split(DropCount, L),
    {Drops2, State#state{time=Time, list=NL, drops=Drops++Drops2,
                         manager_state=NManState}}.

manager_after(State, handle_timeout, _, next) ->
    State;
manager_after(State, handle_timeout, _, post) ->
    {true, State};
manager_after(State, _, Fun, _) ->
    Fun(State).

drops_post(Drops) ->
    lists:all(fun({Sojourn, From, _}) -> drop_post(From, Sojourn) end, Drops).

drop_post({Pid, Tag} = From, Sojourn) ->
    client_msgs(Pid, [{Tag, {drop, Sojourn}}]) andalso no_monitor(From).

post(#state{list=L, outs=Outs, cancels=Cancels, drops=Drops} = State) ->
    lists:all(fun({_, From, _}) -> no_drop_post(From) end,
              L ++ Outs ++ Cancels) andalso
    lists:all(fun({_, From, _}) -> no_monitor(From) end,
              Cancels ++ Drops) andalso
    queue_post(State).

queue_post(#state{list=L, mod=Mod, queue=Q}) ->
    case Mod:len(Q) of
        Len when length(L) == Len ->
            true;
        Len ->
            ct:pal("Len~nExpected: ~p~nObserved: ~p", [length(L), Len]),
            false
    end.

no_drop_post({Pid, _}) ->
    client_msgs(Pid, []).

no_monitor({Pid, _} = Client) ->
    {monitored_by, Pids} = erlang:process_info(Pid, monitored_by),
    case lists:member(self(), Pids) of
        false ->
            true;
        true ->
            ct:pal("Client ~p still monitored", [Client]),
            false
    end.

droplast(List) ->
    [_ | Rest] = lists:reverse(List),
    lists:reverse(Rest).

spawn_client() ->
    Parent = self(),
    spawn(fun() -> client_init(Parent) end).

client_msgs(Pid, Expected) ->
    try gen:call(Pid, call, msgs, 100) of
        {ok, Expected} ->
            true;
        {ok, Observed} ->
            ct:pal("Pid: ~p~nExpected: ~p~nObserved: ~p~n",
                   [Pid, Expected, Observed]),
            false
    catch
        exit:Reason ->
            ct:pal("Pid ~p exit: ~p", [Pid, Reason]),
            false
    end.

client_init(Parent) ->
    Ref = monitor(process, Parent),
    client_loop(Ref, []).

client_loop(Ref, Msgs) ->
    receive
        {'DOWN', Ref, _, _, _} ->
            exit(shutdown);
        {call, From, msgs} ->
            gen:reply(From, Msgs),
            client_loop(Ref, Msgs);
        Msg ->
            client_loop(Ref, Msgs ++ [Msg])
    end.
