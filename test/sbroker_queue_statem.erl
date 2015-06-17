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
-export([in/5]).
-export([shutdown/5]).

-record(state, {manager, manager_state, mod, queue, list=[], outs=[],
                cancels=[], drops=[], out, drop, max, time, send_time}).

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
                manager_state=ManState, out=Out, drop=Drop, max=Max}.

init_post(_, _, _) ->
    true.

in_args(#state{mod=Mod, send_time=SendTime, time=Time, queue=Q}) ->
    [Mod, oneof([SendTime, choose(SendTime, Time)]), tag(), time(Time), Q].

in(Mod, SendTime, Tag, Time, Q) ->
    Pid = spawn_client(),
    From = {Pid, Tag},
    {Pid, Mod:handle_in(SendTime, From, Time, Q)}.

in_pre(#state{mod=Mod, send_time=PrevSendTime, time=PrevTime},
       [Mod2, SendTime, _, Time, _]) ->
    Mod =:= Mod2 andalso SendTime =< Time andalso
    SendTime >= PrevSendTime andalso Time >= PrevTime.

in_next(State, Value,
        [_, SendTime, Tag, Time, _]) ->
    Pid = {call, erlang, element, [1, Value]},
    Elem = {Time - SendTime, {Pid, Tag}},
    Q = {call, erlang, element, [2, Value]},
    NState = State#state{queue=Q, send_time=SendTime},
    case manager_next(NState, handle_timeout, Time) of
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

in_post(State, [_, SendTime, Tag, Time, _], {Pid, Q}) ->
    From = {Pid, Tag},
    Elem = {Time - SendTime, From},
    case manager_post(State#state{queue=Q}, handle_timeout, Time) of
        {true, #state{max=0} = NState} ->
            drop_post(From, Time-SendTime) andalso post(NState);
        {true,
         #state{drop=drop, max=Max, list=[{Sojourn, From2} | NL] = L} = NState}
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
    Q = {call, erlang, element, [{call, erlang, tuple_size, [Value]}, Value]},
    case manager_next(State#state{queue=Q}, handle_out, Time) of
        #state{list=[]} = NState ->
            NState;
        #state{list=[_|L]} = NState ->
            NState#state{list=L}
    end;
handle_out_next(#state{out=out_r} = State, Value, [Time, _]) ->
    Q = {call, erlang, element, [{call, erlang, tuple_size, [Value]}, Value]},
    case manager_next(State#state{queue=Q}, handle_out_r, Time) of
        #state{list=[]} = NState ->
            NState;
        #state{list=L} = NState ->
            NState#state{list=droplast(L)}
    end.

handle_out_post(#state{out=out} = State, [Time, _], {empty, _}) ->
    case manager_post(State, handle_out, Time) of
        {true, #state{list=[]} = NState} ->
            post(NState);
        _ ->
            false
    end;
handle_out_post(#state{out=out} = State, [Time, _], {SendTime, From, _}) ->
    case manager_post(State, handle_out, Time) of
        {true, #state{list=[{Sojourn, From} | NL]} = NState} ->
            Time - SendTime =:= Sojourn andalso post(NState#state{list=NL});
        _ ->
            false
    end;
handle_out_post(#state{out=out_r} = State, [Time, _], {empty, _}) ->
    case manager_post(State, handle_out_r, Time) of
        {true, #state{list=[]}} ->
            true;
        _ ->
            false
    end;
handle_out_post(#state{out=out_r} = State, [Time, _], {SendTime, From, _}) ->
    case manager_post(State, handle_out, Time) of
        {true, #state{list=[_|_] = L} = NState} ->
            {Sojourn, From} = lists:last(L),
            NL = droplast(L),
            Time - SendTime =:= Sojourn andalso post(NState#state{list=NL});
        _ ->
            false
    end;
handle_out_post(_, _, _) ->
    false.

timeout_args(#state{time=Time, queue=Q}) ->
    [time(Time), Q].

handle_timeout_pre(#state{time=PrevTime}, [Time, _]) ->
    Time >= PrevTime.

handle_timeout_next(State, Q, [Time, _]) ->
    manager_next(State#state{queue=Q}, handle_timeout, Time).

handle_timeout_post(State, [Time, _], _) ->
    {Result, _} =  manager_post(State, handle_timeout, Time),
    Result.

cancel_args(#state{time=Time, queue=Q}) ->
    [tag(), time(Time), Q].

handle_cancel_pre(#state{time=PrevTime}, [_, Time, _]) ->
    Time >= PrevTime.

handle_cancel_next(#state{cancels=Cancels} = State, Value, [Tag, Time,  _]) ->
    Q = {call, erlang, element, [2, Value]},
    #state{list=L} = NState = manager_next(State, handle_timeout, Time),
    {NL, Cancels2} = lists:partition(fun({_, {_, Tag2}}) -> Tag =/= Tag2 end,
                                     L),
    NState#state{list=NL, cancels=Cancels++Cancels2, queue=Q}.

handle_cancel_post(#state{cancels=Cancels} = State, [Tag, Time, _],
                   {Cancelled, _}) ->
    {Result, #state{list=L} = NState} =
        manager_post(State, handle_timeout, Time),
    {NL, Cancels2} = lists:partition(fun({_, {_, Tag2}}) -> Tag =/= Tag2 end,
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

handle_info_next(State, Q, [_, Time, _]) ->
    manager_next(State#state{queue=Q}, handle_timeout, Time).

handle_info_post(State, [_, Time, _], _) ->
    {Result, _} =  manager_post(State, handle_timeout, Time),
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
    NQ = Mod:handle_info({'DOWN', make_ref(), process, Pid, shutdown}, Time, Q),
    {ok, NQ};
shutdown(Mod, {_, {Pid, _}}, nomonitor, Time, Q) ->
    NQ = Mod:handle_info({'DOWN', make_ref(), process, Pid, shutdown}, Time, Q),
    {ok, NQ};
shutdown(Mod, {_, {Pid, _}}, monitor, Time, Q) ->
    exit(Pid, shutdown),
    receive
        {'DOWN', _, process, Pid, _} = Down ->
            {ok, Mod:handle_info(Down, Time, Q)}
    after
        5000 ->
            {error, timeout}
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
              [_, {_, From}, _, Time, _]) ->
    Q = {call, erlang, element, [2, Value]},
    #state{list=L, drops=Drops} = NState =
        manager_next(State#state{queue=Q}, handle_timeout, Time),
    Remove = fun({_, From2}) -> From2 =/= From end,
    NL = lists:filter(Remove, L),
    NDrops = lists:filter(Remove, Drops),
    NCancels = lists:filter(Remove, Cancels),
    NOuts = lists:filter(Remove, Outs),
    NDrops = lists:filter(Remove, Drops),
    NState#state{list=NL, drops=NDrops, cancels=NCancels, outs=NOuts, queue=Q};
shutdown_next(State, Value, [_, self, _, Time, _]) ->
    Q = {call, erlang, element, [2, Value]},
    manager_next(State#state{queue=Q}, handle_timeout, Time).

shutdown_post(#state{cancels=Cancels, outs=Outs} = State,
              [_, {_, From}, _, Time, _], {ok, _}) ->
    {Result, #state{list=L, drops=Drops} = NState} =
        manager_post(State, handle_timeout, Time, From),
    Remove = fun({_, From2}) -> From2 =/= From end,
    NL = lists:filter(Remove, L),
    NCancels = lists:filter(Remove, Cancels),
    NOuts = lists:filter(Remove, Outs),
    NDrops = lists:filter(Remove, Drops),
    Result andalso
    post(NState#state{list=NL, cancels=NCancels, outs=NOuts, drops=NDrops});
shutdown_post(State, [_, self, _, Time, _], {ok, _}) ->
    {Result, NState} = manager_post(State, handle_timeout, Time),
    Result andalso post(NState);
shutdown_post(_, [_, {_, {Pid, _}}, _, _, _], {error, timeout}) ->
    ct:pal("Pid ~p DOWN timeout", [Pid]),
    false.

config_change_args(#state{manager=Manager, time=Time, queue=Q}) ->
    [Manager:args(), time(Time), Q].

config_change_pre(#state{time=PrevTime}, [_, Time, _]) ->
    Time >= PrevTime.

config_change_next(#state{manager=Manager, manager_state=ManState,
                          list=L, drops=Drops} = State, Q, [Args, Time, _]) ->
    {Out, Drop, Max, NManState} = Manager:config_change(Time, Args, ManState),
    NState = State#state{queue=Q, manager_state=NManState, out=Out, drop=Drop,
                         max=Max},
    case length(L) of
        Len when Len > Max andalso Drop =:= drop ->
            {Drops2, NL} = lists:split(Len-Max, L),
            NState2 = NState#state{list=NL, drops=Drops++Drops2},
            manager_next(NState2, handle_timeout, Time);
        Len when Len > Max andalso Drop =:= drop_r ->
            {NL, Drops2} = lists:split(Max, L),
            NState2 = NState#state{list=NL, drops=Drops++Drops2},
            manager_next(NState2, handle_timeout, Time);
        _ ->
            manager_next(NState, handle_timeout, Time)
    end.

config_change_post(#state{manager=Manager, manager_state=ManState,
                          list=L, drops=Drops} = State,
                   [Args, Time, _], _) ->
    {Out, Drop, Max, NManState} = Manager:config_change(Time, Args, ManState),
    NState = State#state{manager_state=NManState, out=Out, drop=Drop, max=Max},
    case length(L) of
        Len when Len > Max andalso Drop =:= drop ->
            {Drops2, NL} = lists:split(Len-Max, L),
            NState2 = NState#state{list=NL, drops=Drops++Drops2},
            {Result, NState3} = manager_post(NState2, handle_timeout, Time),
            Result andalso post(NState3);
        Len when Len > Max andalso Drop =:= drop_r ->
            {NL, Drops2} = lists:split(Max, L),
            NState2 = NState#state{list=NL, drops=Drops++Drops2},
            {Result, NState3} = manager_post(NState2, handle_timeout, Time),
            Result andalso post(NState3);
        _ ->
            {Result, NState2} = manager_post(NState, handle_timeout, Time),
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
    [{Time - SendTime , From} || {SendTime, From} <- Result] =:= L.

terminate_args(#state{queue=Q}) ->
    [oneof([shutdown, normal, abnormal]), Q].

terminate_pre(_, _) ->
    true.

terminate_next(#state{manager=Manager}, _, _) ->
    initial_state(Manager).

terminate_post(#state{list=L, cancels=Cancels} = State, _, _) ->
    post(State#state{list=[], cancels=Cancels++L}).

manager_next(State, Fun, Time) ->
    {_, NState} = manager(State, Fun, Time),
    NState.

manager_post(State, Fun, Time) ->
    manager_post(State, Fun, Time, ignore).

manager_post(State, Fun, Time, Skip) ->
    {Drops, NState} = manager(State, Fun, Time),
    Result = lists:all(fun({_, From}) when From =:= Skip ->
                               true;
                          ({Sojourn, From}) ->
                               drop_post(From, Sojourn)
                       end, Drops),
    {Result, NState}.

manager(#state{manager=Manager, manager_state=ManState, list=L, drops=Drops,
               time=PrevTime} = State, Fun, Time) ->
    NL = [{Sojourn + Time - PrevTime, From} || {Sojourn, From} <- L],
    {Sojourns, _} = lists:unzip(NL),
    {DropCount, NManState} = Manager:Fun(Time, Sojourns, ManState),
    {Drops2, NL2} = lists:split(DropCount, NL),
    {Drops2, State#state{time=Time, list=NL2, drops=Drops++Drops2,
                         manager_state=NManState}}.

drop_post({Pid, Tag} = From, Sojourn) ->
    client_msgs(Pid, [{Tag, {drop, Sojourn}}]) andalso no_monitor(From).

post(#state{list=L, outs=Outs, cancels=Cancels, drops=Drops}) ->
    lists:all(fun({_, From}) -> no_drop_post(From) end, L ++ Outs ++ Cancels)
    andalso
    lists:all(fun({_, From}) -> no_monitor(From) end, Outs ++ Cancels ++ Drops).

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
