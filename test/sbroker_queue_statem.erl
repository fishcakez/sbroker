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

-export([initial_state/1]).
-export([command/1]).
-export([precondition/2]).
-export([next_state/3]).
-export([postcondition/3]).

-export([init/3]).
-export([in/6]).
-export([handle_out_queue/1]).
-export([handle_out_timeout/1]).
-export([shutdown/5]).

-record(state, {manager, manager_state, mod, queue, list=[], outs=[],
                cancels=[], drops=[], out, drop, max, time, send_time,
                timeout_time}).

initial_state(Manager) ->
    #state{manager=Manager}.

command(#state{mod=undefined} = State) ->
    {call, ?MODULE, init, init_args(State)};
command(#state{mod=Mod} = State) ->
    frequency([{20, {call, ?MODULE, in, in_args(State)}},
               {10, {call, Mod, handle_out, out_args(State)}},
               {4, {call, Mod, handle_timeout, timeout_args(State)}},
               {4, {call, Mod, handle_cancel, cancel_args(State)}},
               {4, {call, Mod, handle_info, info_args(State)}},
               {4, {call, ?MODULE, shutdown, shutdown_args(State)}},
               {4, {call, Mod, config_change, config_change_args(State)}},
               {4, {call, Mod, len, len_args(State)}},
               {2, {call, Mod, to_list, to_list_args(State)}},
               {1, {call, Mod, terminate, terminate_args(State)}}]).

precondition(State, {call, _, init, Args}) ->
    init_pre(State, Args);
precondition(#state{mod=undefined}, _) ->
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
precondition(State, {call, _, config_change, Args}) ->
    config_change_pre(State, Args);
precondition(State, {call, _, len, Args}) ->
    len_pre(State, Args);
precondition(State, {call, _, to_list, Args}) ->
    to_list_pre(State, Args);
precondition(State, {call, _, terminate, Args}) ->
    terminate_pre(State, Args).

next_state(State, Value, {call, _, init, Args}) ->
    init_next(State, Value, Args);
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
next_state(State, Value, {call, _, config_change, Args}) ->
    config_change_next(State, Value, Args);
next_state(State, Value, {call, _, len, Args}) ->
    len_next(State, Value, Args);
next_state(State, Value, {call, _, to_list, Args}) ->
    to_list_next(State, Value, Args);
next_state(State, Value, {call, _, terminate, Args}) ->
    terminate_next(State, Value, Args).

postcondition(State, {call, _, init, Args}, Result) ->
    init_post(State, Args, Result);
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
postcondition(State, {call, _, config_change, Args}, Result) ->
    config_change_post(State, Args, Result);
postcondition(State, {call, _, len, Args}, Result) ->
    len_post(State, Args, Result);
postcondition(State, {call, _, to_list, Args}, Result) ->
    to_list_post(State, Args, Result);
postcondition(State, {call, _, terminate, Args}, Result) ->
    terminate_post(State, Args, Result).

time() ->
    choose(-10, 10).

time(Time) ->
    oneof([Time,
           ?LET(Incr, choose(5, 5), Time + Incr)]).

tag() ->
    elements([a, b, c]).

init(Mod, Time, Args) ->
    Mod:init(Time, Args).

init_args(#state{manager=Manager}) ->
    [Manager:module(), time(), Manager:args()].

init_pre(#state{mod=Mod}, _) ->
    Mod =:= undefined.

init_next(#state{manager=Manager} = State, Q, [Mod, Time, Args]) ->
    {Out, Drop, Max, ManState} = Manager:init(Args),
    State#state{mod=Mod, queue=Q, time=Time, send_time=Time,
                timeout_time=Time, manager_state=ManState, out=Out,
                drop=Drop, max=Max}.

init_post(_, _, _) ->
    true.

info() ->
    oneof([x, y, z]).

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
    NState = State#state{send_time=SendTime},
    case manager_next(NState, handle_timeout, Time, Q, Timeout) of
        #state{max=0, drops=Drops} = NState2 ->
            NState2#state{drops=Drops ++ [Elem]};
        #state{drop=drop, max=Max, list=L, drops=Drops} = NState2
          when length(L) =:= Max ->
            NState2#state{list=tl(L)++[Elem], drops=Drops ++ [hd(L)]};
        #state{drop=drop_r, max=Max, list=L, drops=Drops} = NState2
          when length(L) =:= Max ->
            NState2#state{drops=Drops ++ [Elem]};
        #state{list=L} = NState2 ->
            NState2#state{list=L++[Elem]}
    end.

in_post(State, [_, SendTime, Tag, Info, Time, _], {Pid, Q, Timeout}) ->
    From = {Pid, Tag},
    Elem = {Time - SendTime, From, Info},
    case manager_post(State, handle_timeout, Time, Q, Timeout) of
        {true, #state{max=0} = NState} ->
            drop_post(From, Time-SendTime) andalso post(NState);
        {true,
         #state{drop=drop, max=Max,
                list=[{Sojourn, From2, _} | NL] = L} = NState}
          when length(L) =:= Max ->
            drop_post(From2, Sojourn) andalso
            post(NState#state{list=NL++[Elem]});
        {true, #state{drop=drop_r, max=Max, list=L} = NState}
          when length(L) =:= Max ->
            drop_post(From, Time-SendTime) andalso post(NState);
        {Result, #state{list=L} = NState} ->
            Result andalso post(NState#state{list=L++[Elem]})
    end.

out_args(#state{time=Time, queue=Q}) ->
    [time(Time), Q].

handle_out_pre(#state{time=PrevTime}, [Time, _]) ->
    Time >= PrevTime.

handle_out_next(#state{out=out} = State, Value, [Time, _]) ->
    Q = {call, ?MODULE, handle_out_queue, [Value]},
    Timeout = {call, ?MODULE, handle_out_timeout, [Value]},
    case manager_next(State, handle_out, Time, Q, Timeout) of
        #state{list=[]} = NState ->
            NState;
        #state{list=[_|L]} = NState ->
            NState#state{list=L}
    end;
handle_out_next(#state{out=out_r} = State, Value, [Time, _]) ->
    Q = {call, ?MODULE, handle_out_queue, [Value]},
    Timeout = {call, ?MODULE, handle_out_timeout, [Value]},
    case manager_next(State, handle_out_r, Time, Q, Timeout) of
        #state{list=[]} = NState ->
            NState;
        #state{list=L} = NState ->
            NState#state{list=droplast(L)}
    end.

handle_out_queue({_, _, _, Q, _}) ->
    Q;
handle_out_queue({empty, Q}) ->
    Q.

handle_out_timeout({_, _, _, _, Timeout}) ->
    Timeout;
handle_out_timeout({empty, _}) ->
    infinity.

handle_out_post(#state{out=out} = State, [Time, _], {empty, Q}) ->
    case manager_post(State, handle_out, Time, Q, infinity) of
        {true, #state{list=[]} = NState} ->
            post(NState);
        _ ->
            false
    end;
handle_out_post(#state{out=out} = State, [Time, _],
                {SendTime, From, Info, Q, Timeout}) ->
    case manager_post(State, handle_out, Time, Q, Timeout) of
        {true, #state{list=[{Sojourn, From, Info} | NL]} = NState} ->
            Time - SendTime =:= Sojourn andalso post(NState#state{list=NL});
        _ ->
            false
    end;
handle_out_post(#state{out=out_r} = State, [Time, _], {empty, Q}) ->
    case manager_post(State, handle_out_r, Time, Q, infinity) of
        {true, #state{list=[]} = NState} ->
            post(NState);
        _ ->
            false
    end;
handle_out_post(#state{out=out_r} = State, [Time, _],
                {SendTime, From, Info, Q, Timeout}) ->
    case manager_post(State, handle_out, Time, Q, Timeout) of
        {true, #state{list=[_|_] = L} = NState} ->
            Sojourn = Time - SendTime,
            NL = droplast(L),
            lists:last(L) =:= {Sojourn, From, Info} andalso
            post(NState#state{list=NL});
        _ ->
            false
    end;
handle_out_post(_, _, _) ->
    false.

timeout_args(#state{time=Time, queue=Q}) ->
    [time(Time), Q].

handle_timeout_pre(#state{time=PrevTime}, [Time, _]) ->
    Time >= PrevTime.

handle_timeout_next(State, Value, [Time, _]) ->
    Q = {call, erlang, element, [1, Value]},
    Timeout = {call, erlang, element, [2, Value]},
    manager_next(State, handle_timeout, Time, Q, Timeout).

handle_timeout_post(State, [Time, _], {Q, Timeout}) ->
    {Result, _} =  manager_post(State, handle_timeout, Time, Q, Timeout),
    Result.

cancel_args(#state{time=Time, queue=Q}) ->
    [tag(), time(Time), Q].

handle_cancel_pre(#state{time=PrevTime}, [_, Time, _]) ->
    Time >= PrevTime.

handle_cancel_next(#state{cancels=Cancels} = State, Value, [Tag, Time,  _]) ->
    Q = {call, erlang, element, [2, Value]},
    Timeout = {call, erlang, element, [3, Value]},
    #state{list=L} = NState =
        manager_next(State, handle_timeout, Time, Q, Timeout),
    {NL, Cancels2} = lists:partition(fun({_, {_, Tag2}, _}) -> Tag =/= Tag2 end,
                                     L),
    NState#state{list=NL, cancels=Cancels++Cancels2, queue=Q}.

handle_cancel_post(#state{cancels=Cancels} = State, [Tag, Time, _],
                   {Cancelled, Q, Timeout}) ->
    {Result, #state{list=L} = NState} =
        manager_post(State, handle_timeout, Time, Q, Timeout),
    {NL, Cancels2} = lists:partition(fun({_, {_, Tag2}, _}) -> Tag =/= Tag2 end,
                                     L),
    NState2 = NState#state{list=NL, cancels=Cancels ++ Cancels2},
    case length(Cancels2) of
        0 when Cancelled =:= false ->
            Result andalso post(NState2);
        N when N =:= Cancelled ->
            Result andalso post(NState2);
        _ ->
            false
    end.

info_args(#state{time=Time, queue=Q}) ->
    [tag(), time(Time), Q].

handle_info_pre(#state{time=PrevTime}, [_, Time, _]) ->
    Time >= PrevTime.

handle_info_next(State, Value, [_, Time, _]) ->
    Q = {call, erlang, element, [1, Value]},
    Timeout = {call, erlang, element, [2, Value]},
    manager_next(State, handle_timeout, Time, Q, Timeout).

handle_info_post(State, [_, Time, _], {Q, Timeout}) ->
    {Result, _} =  manager_post(State, handle_timeout, Time, Q, Timeout),
    Result.

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

shutdown_next(#state{cancels=Cancels, outs=Outs} = State, Value,
              [_, {_, From, _}, _, Time, _]) ->
    Q = {call, erlang, element, [1, Value]},
    Timeout = {call, erlang, element, [2, Value]},
    #state{list=L, drops=Drops} = NState =
        manager_next(State, handle_timeout, Time, Q, Timeout),
    Remove = fun({_, From2, _}) -> From2 =/= From end,
    NL = lists:filter(Remove, L),
    NDrops = lists:filter(Remove, Drops),
    NCancels = lists:filter(Remove, Cancels),
    NOuts = lists:filter(Remove, Outs),
    NDrops = lists:filter(Remove, Drops),
    NState#state{list=NL, drops=NDrops, cancels=NCancels, outs=NOuts, queue=Q};
shutdown_next(State, Value, [_, self, _, Time, _]) ->
    Q = {call, erlang, element, [1, Value]},
    Timeout = {call, erlang, element, [2, Value]},
    manager_next(State, handle_timeout, Time, Q, Timeout).

shutdown_post(#state{cancels=Cancels, outs=Outs} = State,
              [_, {_, From, _}, _, Time, _], {Q, Timeout}) ->
    {Result, #state{list=L, drops=Drops} = NState} =
        manager_post(State, handle_timeout, Time, Q, Timeout, From),
    Remove = fun({_, From2, _}) -> From2 =/= From end,
    NL = lists:filter(Remove, L),
    NCancels = lists:filter(Remove, Cancels),
    NOuts = lists:filter(Remove, Outs),
    NDrops = lists:filter(Remove, Drops),
    Result andalso
    post(NState#state{list=NL, cancels=NCancels, outs=NOuts, drops=NDrops});
shutdown_post(State, [_, self, _, Time, _], {Q, Timeout}) ->
    {Result, NState} = manager_post(State, handle_timeout, Time, Q, Timeout),
    Result andalso post(NState);
shutdown_post(_, [_, {_, {Pid, _}, _}, _, _, _], timeout) ->
    ct:pal("Pid ~p DOWN timeout", [Pid]),
    false.

config_change_args(#state{manager=Manager, time=Time, queue=Q}) ->
    [Manager:args(), time(Time), Q].

config_change_pre(#state{time=PrevTime}, [_, Time, _]) ->
    Time >= PrevTime.

config_change_next(#state{manager=Manager, manager_state=ManState,
                          list=L, drops=Drops} = State,
                   Value, [Args, Time, _]) ->
    Q = {call, erlang, element, [1, Value]},
    Timeout = {call, erlang, element, [2, Value]},
    {Out, Drop, Max, NManState} = Manager:config_change(Time, Args, ManState),
    NState = State#state{manager_state=NManState, out=Out, drop=Drop, max=Max},
    case length(L) of
        Len when Len > Max andalso Drop =:= drop ->
            {Drops2, NL} = lists:split(Len-Max, L),
            NState2 = NState#state{list=NL, drops=Drops++Drops2},
            manager_next(NState2, handle_timeout, Time, Q, Timeout);
        Len when Len > Max andalso Drop =:= drop_r ->
            {NL, Drops2} = lists:split(Max, L),
            NState2 = NState#state{list=NL, drops=Drops++Drops2},
            manager_next(NState2, handle_timeout, Time, Q, Timeout);
        _ ->
            manager_next(NState, handle_timeout, Time, Q, Timeout)
    end.

config_change_post(#state{manager=Manager, manager_state=ManState,
                          list=L, drops=Drops} = State,
                   [Args, Time, _], {Q, Timeout}) ->
    {Out, Drop, Max, NManState} = Manager:config_change(Time, Args, ManState),
    NState = State#state{manager_state=NManState, out=Out, drop=Drop, max=Max,
                         timeout_time=Time},
    case length(L) of
        Len when Len > Max andalso Drop =:= drop ->
            {Drops2, NL} = lists:split(Len-Max, L),
            NState2 = NState#state{list=NL, drops=Drops++Drops2},
            {Result, NState3} =
                manager_post(NState2, handle_timeout, Time, Q, Timeout),
            Result andalso post(NState3);
        Len when Len > Max andalso Drop =:= drop_r ->
            {NL, Drops2} = lists:split(Max, L),
            NState2 = NState#state{list=NL, drops=Drops++Drops2},
            {Result, NState3} =
                manager_post(NState2, handle_timeout, Time, Q, Timeout),
            Result andalso post(NState3);
        _ ->
            {Result, NState2} =
                manager_post(NState, handle_timeout, Time, Q, Timeout),
            Result andalso post(NState2)
    end.

len_args(#state{queue=Q}) ->
    [Q].

len_pre(_, _) ->
    true.

len_next(State, _, _) ->
    State.

len_post(#state{list=L}, _, Len) ->
    length(L) =:= Len.

to_list_args(#state{queue=Q}) ->
    [Q].

to_list_pre(_, _) ->
    true.

to_list_next(State, _, _) ->
    State.

to_list_post(#state{time=Time, list=L}, _, Result) ->
    [{Time - SendTime , From, Info} || {SendTime, From, Info} <- Result] =:= L.

terminate_args(#state{queue=Q}) ->
    [oneof([shutdown, normal, abnormal]), Q].

terminate_pre(_, _) ->
    true.

terminate_next(#state{manager=Manager}, _, _) ->
    initial_state(Manager).

terminate_post(#state{list=L, cancels=Cancels} = State, _, _) ->
    post(State#state{list=[], cancels=Cancels++L}).

manager_next(State, Fun, Time, Q, Timeout) ->
    NState = State#state{queue=Q, timeout_time=Timeout},
    {_, NState2} = manager(NState, Fun, Time),
    NState2.

manager_post(State, Fun, Time, Q, Timeout) ->
    manager_post(State, Fun, Time, Q, Timeout, ignore).

manager_post(#state{manager=Manager, timeout_time=PrevTimeout} = State, Fun,
             Time, Q, Timeout, Skip) ->
    NState = State#state{queue=Q, timeout_time=Timeout},
    {Drops, #state{list=L} = NState2} = manager(NState, Fun, Time),
    Result = lists:all(fun({_, From, _}) when From =:= Skip ->
                               true;
                          ({Sojourn, From, _}) ->
                               drop_post(From, Sojourn)
                       end, Drops),
    case Manager:time_dependence() of
        dependent when PrevTimeout > Time andalso Drops =/= [] ->
            {false, NState2};
        dependent when Timeout < Time ->
            {false, NState2};
        dependent when Timeout =:= infinity andalso L =/= [] ->
            {false, NState2};
        dependent when not (is_integer(Timeout) orelse Timeout =:= infinity) ->
            {false, NState2};
        independent when Timeout =/= infinity ->
            {false, NState2};
        _ ->
            {Result, NState2}
    end.

manager(#state{manager=Manager, manager_state=ManState, list=L, drops=Drops,
               time=PrevTime} = State, Fun, Time) ->
    NL = [{Sojourn + Time - PrevTime, From, Info} ||
          {Sojourn, From, Info} <- L],
    {Sojourns, _, _} = lists:unzip3(NL),
    {DropCount, NManState} = Manager:Fun(Time, Sojourns, ManState),
    {Drops2, NL2} = lists:split(DropCount, NL),
    {Drops2, State#state{time=Time, list=NL2, drops=Drops++Drops2,
                         manager_state=NManState}}.

drop_post({Pid, Tag} = From, Sojourn) ->
    client_msgs(Pid, [{Tag, {drop, Sojourn}}]) andalso no_monitor(From).

post(#state{list=L, outs=Outs, cancels=Cancels, drops=Drops}) ->
    lists:all(fun({_, From, _}) -> no_drop_post(From) end,
              L ++ Outs ++ Cancels) andalso
    lists:all(fun({_, From, _}) -> no_monitor(From) end,
              Outs ++ Cancels ++ Drops).

no_drop_post({Pid, _}) ->
    client_msgs(Pid, []).

no_monitor({Pid, _}) ->
    {monitored_by, Pids} = erlang:process_info(Pid, monitored_by),
    not lists:member(self(), Pids).

droplast(List) ->
    [_ | Rest] = lists:reverse(List),
    lists:reverse(Rest).

spawn_client() ->
    Parent = self(),
    spawn(fun() -> client_init(Parent) end).

client_msgs(Pid, Expected) ->
    try gen:call(Pid, call, msgs, 5000) of
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
