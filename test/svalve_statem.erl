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
-module(svalve_statem).

-include_lib("proper/include/proper.hrl").

-compile({no_auto_import, [time/0]}).

-export([initial_state/1]).
-export([command/1]).
-export([precondition/2]).
-export([next_state/3]).
-export([postcondition/3]).

-export([new/3]).
-export([new/4]).
-export([drop/1]).
-export([drop_r/1]).
-export([join/5]).
-export([filter/3]).
-export([filter/4]).

-record(state, {manager, manager_state, status=open, time=0, tail_time=0,
                manager_time=0, list=[], squeue_drops=[], squeue_state=[],
                queue}).

initial_state(Manager) ->
    #state{manager=Manager}.

command(#state{queue=undefined} = State) ->
    {call, ?MODULE, new, new_args(State)};
command(State) ->
    frequency([{30, {call, svalve, in, in_args(State)}},
               {15, {call, svalve, sojourn, sojourn_args(State)}},
               {15, {call, svalve, sojourn_r, sojourn_r_args(State)}},
               {6, {call, svalve, out, out_args(State)}},
               {6, {call, svalve, out_r, out_r_args(State)}},
               {6, {call, ?MODULE, drop, drop_args(State)}},
               {6, {call, ?MODULE, drop_r, drop_r_args(State)}},
               {6, {call, ?MODULE, join, join_args(State)}},
               {4, {call, svalve, dropped, dropped_args(State)}},
               {4, {call, svalve, dropped_r, dropped_r_args(State)}},
               {4, {call, ?MODULE, filter, filter_args(State)}},
               {4, {call, svalve, open, open_args(State)}},
               {4, {call, svalve, close, close_args(State)}},
               {1, {call, svalve, time, time_args(State)}},
               {1, {call, svalve, timeout, timeout_args(State)}},
               {1, {call, svalve, len, len_args(State)}},
               {1, {call, svalve, to_list, to_list_args(State)}}]).

precondition(State, {call, _, new, Args}) ->
    new_pre(State, Args);
precondition(#state{queue=undefined}, _) ->
    false;
precondition(State, {call, _, in, Args}) ->
    in_pre(State, Args);
precondition(State, {call, _, out, Args}) ->
    out_pre(State, Args);
precondition(State, {call, _, out_r, Args}) ->
    out_r_pre(State, Args);
precondition(State, {call, _, drop, Args}) ->
    drop_pre(State, Args);
precondition(State, {call, _, drop_r, Args}) ->
    drop_r_pre(State, Args);
precondition(State, {call, _, join, Args}) ->
    join_pre(State, Args);
precondition(State, {call, _, filter, Args}) ->
    filter_pre(State, Args);
precondition(State, {call, _, time, Args}) ->
    time_pre(State, Args);
precondition(State, {call, _, timeout, Args}) ->
    timeout_pre(State, Args);
precondition(State, {call, _, len, Args}) ->
    len_pre(State, Args);
precondition(State, {call, _, to_list, Args}) ->
    to_list_pre(State, Args);
precondition(State, {call, _, sojourn, Args}) ->
    sojourn_pre(State, Args);
precondition(State, {call, _, sojourn_r, Args}) ->
    sojourn_r_pre(State, Args);
precondition(State, {call, _, dropped, Args}) ->
    dropped_pre(State, Args);
precondition(State, {call, _, dropped_r, Args}) ->
    dropped_r_pre(State, Args);
precondition(State, {call, _, open, Args}) ->
    open_pre(State, Args);
precondition(State, {call, _, close, Args}) ->
    close_pre(State, Args).

next_state(State, Value, {call, _, new, Args}) ->
    new_next(State, Value, Args);
next_state(State, Value, {call, _, in, Args}) ->
    in_next(State, Value, Args);
next_state(State, Value, {call, _, out, Args}) ->
    out_next(State, Value, Args);
next_state(State, Value, {call, _, out_r, Args}) ->
    out_r_next(State, Value, Args);
next_state(State, Value, {call, _, drop, Args}) ->
    drop_next(State, Value, Args);
next_state(State, Value, {call, _, drop_r, Args}) ->
    drop_r_next(State, Value, Args);
next_state(State, Value, {call, _, join, Args}) ->
    join_next(State, Value, Args);
next_state(State, Value, {call, _, filter, Args}) ->
    filter_next(State, Value, Args);
next_state(State, Value, {call, _, time, Args}) ->
    time_next(State, Value, Args);
next_state(State, Value, {call, _, timeout, Args}) ->
    timeout_next(State, Value, Args);
next_state(State, Value, {call, _, len, Args}) ->
    len_next(State, Value, Args);
next_state(State, Value, {call, _, to_list, Args}) ->
    to_list_next(State, Value, Args);
next_state(State, Value, {call, _, sojourn, Args}) ->
    sojourn_next(State, Value, Args);
next_state(State, Value, {call, _, sojourn_r, Args}) ->
    sojourn_r_next(State, Value, Args);
next_state(State, Value, {call, _, dropped, Args}) ->
    dropped_next(State, Value, Args);
next_state(State, Value, {call, _, dropped_r, Args}) ->
    dropped_r_next(State, Value, Args);
next_state(State, Value, {call, _, open, Args}) ->
    open_next(State, Value, Args);
next_state(State, Value, {call, _, close, Args}) ->
    close_next(State, Value, Args).

postcondition(State, {call, _, new, Args}, Result) ->
    new_post(State, Args, Result);
postcondition(State, {call, _, in, Args}, Result) ->
    in_post(State, Args, Result);
postcondition(State, {call, _, out, Args}, Result) ->
    out_post(State, Args, Result);
postcondition(State, {call, _, out_r, Args}, Result) ->
    out_r_post(State, Args, Result);
postcondition(State, {call, _, drop, Args}, Result) ->
    drop_post(State, Args, Result);
postcondition(State, {call, _, drop_r, Args}, Result) ->
    drop_r_post(State, Args, Result);
postcondition(State, {call, _, join, Args}, Result) ->
    join_post(State, Args, Result);
postcondition(State, {call, _, filter, Args}, Result) ->
    filter_post(State, Args, Result);
postcondition(State, {call, _, time, Args}, Result) ->
    time_post(State, Args, Result);
postcondition(State, {call, _, timeout, Args}, Result) ->
    timeout_post(State, Args, Result);
postcondition(State, {call, _, len, Args}, Result) ->
    len_post(State, Args, Result);
postcondition(State, {call, _, to_list, Args}, Result) ->
    to_list_post(State, Args, Result);
postcondition(State, {call, _, sojourn, Args}, Result) ->
    sojourn_post(State, Args, Result);
postcondition(State, {call, _, sojourn_r, Args}, Result) ->
    sojourn_r_post(State, Args, Result);
postcondition(State, {call, _, dropped, Args}, Result) ->
    dropped_post(State, Args, Result);
postcondition(State, {call, _, dropped_r, Args}, Result) ->
    dropped_r_post(State, Args, Result);
postcondition(State, {call, _, open, Args}, Result) ->
    open_post(State, Args, Result);
postcondition(State, {call, _, close, Args}, Result) ->
    close_post(State, Args, Result).

time() ->
    choose(-10, 10).

item() ->
    oneof([a, b, c]).

time(Time) ->
    frequency([{10, ?LET(Incr, choose(0, 3), Time + Incr)},
               {1, choose(-10, Time)}]).

new(Manager, Args, SDrops) ->
    V = svalve:new(Manager, Args),
    S = squeue:new(sbroker_statem_queue, SDrops),
    svalve:squeue(S, V).

new(Time, Manager, Args, SDrops) ->
    V = svalve:new(Time, Manager, Args),
    S = squeue:new(Time, sbroker_statem_queue, SDrops),
    svalve:squeue(S, V).

new_args(#state{manager=Manager}) ->
    Args = [Manager:module(), Manager:args(), squeue_drops()],
    oneof([[time() | Args], Args]).

squeue_drops() ->
    resize(4, list(oneof([0, choose(1, 2)]))).

new_pre(#state{queue=Q}, _Args) ->
    Q =:= undefined.

new_next(State, Value, [Module, Args, SDrops]) ->
    new_next(State, Value, [0, Module, Args, SDrops]);
new_next(#state{manager=Manager} = State, VQ, [Time, _Module, Args, SDrops]) ->
    State#state{time=Time, tail_time=Time, manager_time=Time, queue=VQ, list=[],
                manager_state=Manager:init(Args), squeue_drops=SDrops,
                squeue_state=SDrops}.

new_post(_, _Args, Q) ->
    svalve:is_queue(Q).

in_args(#state{time=Time, queue=Q} = State) ->
    oneof([in_args_intime(State),
           [time(Time), item(), Q],
           [item(), Q]]).

in_args_intime(#state{time=PrevTime, queue=Q}) ->
    ?SUCHTHAT([Time, InTime, _Item, _Q],
              [time(PrevTime), oneof([time(), time(PrevTime)]), item(), Q],
              Time >= InTime).

in_pre(_State, [Time, InTime, _Item, _Q]) when InTime > Time ->
    false;
in_pre(_State, _Args) ->
    true.

in_next(#state{time=Time} = State, Value, [Item, Q]) ->
    in_next(State, Value, [Time, Item, Q]);
in_next(#state{time=PrevTime} = State, Value, [Time, Item, Q]) ->
    NTime = max(PrevTime, Time),
    in_next(State, Value, [NTime, NTime, Item, Q]);
in_next(#state{tail_time=TailTime} = State, Value,
        [Time, InTime, Item, _Q]) ->
    VQ = {call, erlang, element, [2, Value]},
    #state{time=NTime, list=L} = NState = advance_time_state(State, Time),
    NTailTime = max(TailTime, InTime),
    SojournTime = NTime - NTailTime,
    NState#state{tail_time=NTailTime,  list=L ++ [{SojournTime, Item}],
                 queue=VQ}.

in_post(#state{time=Time} = State, [Item, Q], Result) ->
    in_post(State, [Time, Item, Q], Result);
in_post(#state{time=PrevTime} = State, [Time, Item, Q], Result) ->
    NTime = max(PrevTime, Time),
    in_post(State, [NTime, NTime, Item, Q], Result);
in_post(State, [Time, _InTime, _Item, _Q], {Drops, NQ}) ->
    advance_time_drops(State, Time) =:= Drops andalso svalve:is_queue(NQ).

out_args(#state{time=Time, queue=Q}) ->
    oneof([[time(Time), Q],
           [Q]]).

out_pre(_State, _Args) ->
    true.

out_next(#state{time=Time} = State, Value, [Q]) ->
    out_next(State, Value, [Time, Q]);
out_next(State, Value, [Time, _Q]) ->
    VQ = {call, erlang, element, [3, Value]},
    case advance_time_state(State, Time) of
        #state{list=[]} = NState ->
            NState#state{queue=VQ};
        #state{list=[_Out | NL]} = NState ->
            NState#state{list=NL, queue=VQ}
    end.

out_post(#state{time=Time} = State, [Q], Result) ->
    out_post(State, [Time, Q], Result);
out_post(State, [Time, _Q], {Result, Drops, NQ}) ->
    case advance_time(State, Time) of
        {Drops, #state{list=[]}} ->
            Result =:= empty andalso svalve:is_queue(NQ);
        {Drops, #state{list=[Result|_]}} ->
            svalve:is_queue(NQ);
        _ ->
            false
    end.

out_r_args(State) ->
    out_args(State).

out_r_pre(State, Args) ->
    out_pre(State, Args).

out_r_next(#state{time=Time} = State, Value, [Q]) ->
    out_r_next(State, Value, [Time, Q]);
out_r_next(State, Value, [Time, _Q]) ->
    VQ = {call, erlang, element, [3, Value]},
    case advance_time_state(State, Time) of
        #state{list=[]} = NState ->
            NState#state{queue=VQ};
        #state{list=L} = NState ->
            NState#state{list=droplast(L), queue=VQ}
    end.

out_r_post(#state{time=Time} = State, [Q], Result) ->
    out_r_post(State, [Time, Q], Result);
out_r_post(State, [Time, _Q], {Result, Drops, NQ}) ->
    case advance_time(State, Time) of
        {Drops, #state{list=[]}} ->
            Result =:= empty andalso svalve:is_queue(NQ);
        {Drops, #state{list=L}} ->
            Result =:= lists:last(L) andalso svalve:is_queue(NQ);
        {Drops2, State2} ->
            ct:pal("Drops: ~p~nState:~p",[Drops2, State2]),
            false
    end.

drop(Args) ->
    try apply(svalve, drop, Args) of
        Result ->
            Result
    catch
        Class:Reason ->
            {Class, Reason, erlang:get_stacktrace()}
    end.

drop_args(#state{time=Time, queue=Q}) ->
    [oneof([[time(Time), Q],
                 [Q]])].

drop_pre(_, _) ->
    true.

drop_next(#state{list=[]} = State, _Value, _Args) ->
    State;
drop_next(#state{time=Time} = State, Value, [[Q]]) ->
    drop_next(State, Value, [[Time, Q]]);
drop_next(State, Value, [[Time, _Q]]) ->
    VQ = {call, erlang, element, [2, Value]},
    case advance_time(State, Time) of
        {[], #state{list=[_|NL]} = NState} ->
            NState#state{list=NL, queue=VQ};
        {_, NState} ->
            NState#state{queue=VQ}
    end.

drop_post(#state{list=[]}, _, Result) ->
    case Result of
       {error, empty, [{squeue, drop, _, _} | _]} ->
            true;
        _ ->
            false
    end;
drop_post(#state{time=Time} = State, [[Q]], Result) ->
    drop_post(State, [[Time, Q]], Result);
drop_post(State, [[Time, _Q]], {Drops, NQ}) ->
    case advance_time(State, Time) of
        {[], #state{list=[Drop|_]}} ->
            Drops =:= [Drop] andalso svalve:is_queue(NQ);
        {Drops, _} ->
            svalve:is_queue(NQ);
        _ ->
            false
    end.

drop_r(Args) ->
    try apply(svalve, drop_r, Args) of
        Result ->
            Result
    catch
        Class:Reason ->
            {Class, Reason, erlang:get_stacktrace()}
    end.

drop_r_args(State) ->
    drop_args(State).

drop_r_pre(State, Args) ->
    drop_pre(State, Args).

drop_r_next(#state{list=[]} = State, _Value, _Args) ->
    State;
drop_r_next(#state{time=Time} = State, Value, [[Q]]) ->
    drop_r_next(State, Value, [[Time, Q]]);
drop_r_next(State, Value, [[Time, _Q]]) ->
    VQ = {call, erlang, element, [2, Value]},
    case advance_time(State, Time) of
        {[], #state{list=NL} = NState} ->
            NState#state{list=droplast(NL), queue=VQ};
        {_, NState} ->
            NState#state{queue=VQ}
    end.

drop_r_post(#state{list=[]}, _, Result) ->
    case Result of
        {error, empty, [{squeue, drop_r, _, _} | _]} ->
            true;
        _ ->
            false
    end;
drop_r_post(#state{time=Time} = State, [[Q]], Result) ->
    drop_r_post(State, [[Time, Q]], Result);
drop_r_post(State, [[Time, _Q]], {Drops, NQ}) ->
    case advance_time(State, Time) of
        {[], #state{list=L}} ->
            Drops =:= [lists:last(L)] andalso svalve:is_queue(NQ);
        {Drops, _} ->
            svalve:is_queue(NQ);
        _ ->
            false
    end.

join(Q1, MTime, Module, Args, {Time, TailTime, StartList, SDrops}) ->
    S = squeue:from_start_list(Time, TailTime, StartList, sbroker_statem_queue,
                               SDrops),
    Q2 = svalve:squeue(S, svalve:time(Time, svalve:new(MTime, Module, Args))),
    svalve:join(Q1, Q2).

join_list(MinStart, MaxStart) ->
    ?LET(Unordered, list(join_item(MinStart, MaxStart)),
         lists:sort(fun start_sort/2, Unordered)).

start_sort({T1, _Item1}, {T2, _Item2}) ->
    T1 =< T2.

join_item(MinStart, MaxStart) ->
    {choose(MinStart, MaxStart), item()}.

join_queue(Time, MinStart) ->
    ?LET({JoinList, SDrops}, {join_list(MinStart, Time), squeue_drops()},
         begin
             {MaxItemTime, _} = lists:last([{-10, a} | JoinList]),
             TailTime = choose(MaxItemTime, Time),
             {Time, TailTime, JoinList, SDrops}
         end).

join_args(#state{manager=Manager, list=L, time=Time, queue=Q}) ->
    %% Add default item that has been in queue whole time so that empty queue
    %% has MinStart of -10.
    {SojournTime, _} = lists:last([{Time+10, a} | L]),
    MinStart = Time-SojournTime,
    [Q, choose(-10, Time), Manager:module(), Manager:args(),
     join_queue(Time, MinStart)].


join_pre(#state{time=Time, list=L1}, [_Q1, _, _, _, {Time, _, StartList, _}]) ->
     L2 = [{Time - Start, Item} || {Start, Item} <- StartList],
     do_join_pre(L1, L2);
join_pre(_State, _Args) ->
    false.

do_join_pre(L1,L2) ->
    case {L1, L2} of
        {[], _} ->
            true;
        {_, []} ->
            true;
        {_, [MaxSojourn2 | _]} ->
            {MinSojourn1, _Item} = lists:last(L1),
            %% Queues merge with L1 at head and L2 at tail. Order must be
            %% maintained so that sojourn time is decreasing from head to tail.
            %% Therefore the tail of L1 must have a sojourn time greater than or
            %% equal to L2
           MinSojourn1 >=  MaxSojourn2
    end.

join_next(#state{time=Time, tail_time=TailTime, manager_time=MTime,
                 list=L1} = State, VQ,
          [_Q1, MTime2, _, _, {Time, TailTime2, StartList, _}]) ->
    L2 = [{Time - Start, Item} || {Start, Item} <- StartList],
    State#state{tail_time=max(TailTime, TailTime2),
                manager_time=max(MTime, MTime2), list=L1++L2, queue=VQ}.

join_post(_, _, NQ) ->
    svalve:is_queue(NQ).

filter(Method, Item, Q) ->
    svalve:filter(make_filter(Method, Item), Q).

filter(Time, Method, Item, Q) ->
    svalve:filter(Time, make_filter(Method, Item), Q).

make_filter(duplicate, Item) ->
    fun(Item2) when Item2 =:= Item ->
            [Item2, Item2];
       (_Other) ->
            true
    end;
make_filter(filter, Item) ->
    fun(Item2) ->
            Item2 =:= Item
    end.

filter_args(#state{time=Time, queue=Q}) ->
    Args = [oneof([duplicate, filter]), item(), Q],
    oneof([[time(Time) | Args], Args]).

filter_pre(_State, _Args) ->
    true.

filter_next(#state{time=Time} = State, Value, [Action, Item, Q]) ->
    filter_next(State, Value, [Time, Action, Item, Q]);
filter_next(State, Value, [Time, duplicate, Item, _Q]) ->
    VQ = {call, erlang, element, [2, Value]},
    #state{list=L} = NState = advance_time_state(State, Time),
    Duplicate = fun({_, Item2} = Elem) when Item2 =:= Item ->
                        [Elem, Elem];
                   (Other) ->
                        [Other]
                end,
    NL = lists:flatmap(Duplicate, L),
    NState#state{list=NL, queue=VQ};
filter_next(State, Value, [Time, filter, Item, _Q]) ->
    VQ = {call, erlang, element, [2, Value]},
    #state{list=L} = NState = advance_time_state(State, Time),
    Filter = fun({_, Item2} ) -> Item2 =:= Item end,
    NL = lists:filter(Filter, L),
    NState#state{list=NL, queue=VQ}.

filter_post(#state{time=Time} = State, [Action, Item, Q], Result) ->
    filter_post(State, [Time, Action, Item, Q], Result);
filter_post(State, [Time, _Action, _Item, _Q], {Drops, NQ}) ->
    advance_time_drops(State, Time) =:= Drops andalso svalve:is_queue(NQ).

time_args(#state{queue=Q}) ->
    [Q].

time_pre(_State, _Args) ->
    true.

time_next(State, _Value, _Args) ->
    State.

time_post(#state{time=Time}, _Args, Time2) ->
    Time =:= Time2.

timeout_args(#state{time=Time, queue=Q}) ->
    [time(Time), Q].

timeout_pre(_State, _Args) ->
      true.

timeout_next(State, Value, [Time, _Q]) ->
    VQ = {call, erlang, element, [2, Value]},
    NState = advance_time_state(State, Time),
    NState#state{queue=VQ}.

timeout_post(State, [Time, _Q], {Drops, NQ}) ->
    advance_time_drops(State, Time) =:= Drops andalso svalve:is_queue(NQ).

len_args(#state{queue=Q}) ->
    [Q].

len_pre(_State, _Args) ->
    true.

len_next(State, _Value, _Args) ->
    State.

len_post(#state{list=L}, [_S], Len) ->
    length(L) =:= Len.

to_list_args(#state{queue=Q}) ->
    [Q].

to_list_pre(_State, _Args) ->
    true.

to_list_next(State, _Value, _Args) ->
    State.

to_list_post(#state{list=L}, [_Q], List) ->
    {_, Items} = lists:unzip(L),
    Items =:= List.

%% svalve

sojourn() ->
    choose(0, 3).

sojourn_args(#state{time=Time, queue=Q} = State) ->
    oneof([sojourn_args_outtime(State),
           [time(Time), sojourn(), Q],
           [sojourn(), Q]]).

sojourn_args_outtime(#state{time=PrevTime, queue=Q}) ->
    ?SUCHTHAT([Time, OutTime, _Sojourn, _Q],
              [time(PrevTime), oneof([time(), time(PrevTime)]), sojourn(), Q],
              Time >= OutTime).

sojourn_pre(_State, _Args) ->
    true.

sojourn_next(#state{time=Time} = State, Value, [SojournTime, Q]) ->
    sojourn_next(State, Value, [Time, SojournTime, Q]);
sojourn_next(State, Value, [Time, SojournTime, Q]) ->
    sojourn_next(State, Value, [Time, Time, SojournTime, Q]);
sojourn_next(State, Value, [Time, OutTime, SojournTime, _Q]) ->
    VQ = {call, erlang, element, [3, Value]},
    case prepare_sojourn(State, OutTime, SojournTime, Time) of
        {closed, _, NState} ->
            NState#state{queue=VQ};
        {open, _, #state{list=[]} = NState} ->
            NState#state{queue=VQ};
        {open, _, #state{list=[_Out | NL]} = NState} ->
            NState#state{list=NL, queue=VQ}
    end.

sojourn_post(#state{time=Time} = State, [SojournTime, Q], Result) ->
    sojourn_post(State, [Time, SojournTime, Q], Result);
sojourn_post(State, [Time, SojournTime, Q], Result) ->
    sojourn_post(State, [Time, Time, SojournTime, Q], Result);
sojourn_post(State, [Time, OutTime, SojournTime, _Q], {Result, Drops, NQ}) ->
    case prepare_sojourn(State, OutTime,SojournTime, Time) of
        {closed, Drops, _} ->
            Result =:= closed andalso svalve:is_queue(NQ);
        {open, Drops, #state{list=[]}} ->
            Result =:= empty andalso svalve:is_queue(NQ);
        {open, Drops, #state{list=[Result|_]}} ->
            svalve:is_queue(NQ);
        Result2 ->
            ct:pal("~p", [Result2]),
            false
    end.

sojourn_r_args(State) ->
    sojourn_args(State).

sojourn_r_pre(State, Args) ->
    sojourn_pre(State, Args).

sojourn_r_next(#state{time=Time} = State, Value, [SojournTime, Q]) ->
    sojourn_r_next(State, Value, [Time, SojournTime, Q]);
sojourn_r_next(State, Value, [Time, SojournTime, Q]) ->
    sojourn_r_next(State, Value, [Time, Time, SojournTime, Q]);
sojourn_r_next(State, Value, [Time, OutTime, SojournTime, _Q]) ->
    VQ = {call, erlang, element, [3, Value]},
    case prepare_sojourn(State, OutTime, SojournTime, Time) of
        {closed, _, NState} ->
            NState#state{queue=VQ};
        {open, _, #state{list=[]} = NState} ->
            NState#state{queue=VQ};
        {open, _, #state{list=NL} = NState} ->
            NState#state{list=droplast(NL), queue=VQ}
    end.

sojourn_r_post(#state{time=Time} = State, [SojournTime, Q], Result) ->
    sojourn_r_post(State, [Time, SojournTime, Q], Result);
sojourn_r_post(State, [Time, SojournTime, Q], Result) ->
    sojourn_r_post(State, [Time, Time, SojournTime, Q], Result);
sojourn_r_post(State, [Time, OutTime, SojournTime, _Q], {Result, Drops, NQ}) ->
    case prepare_sojourn(State, OutTime, SojournTime, Time) of
        {closed, Drops, _} ->
            Result =:= closed andalso svalve:is_queue(NQ);
        {open, Drops, #state{list=[]}} ->
            Result =:= empty andalso svalve:is_queue(NQ);
        {open, Drops, #state{list=NL}} ->
            lists:last(NL) =:= Result andalso svalve:is_queue(NQ);
        Result2 ->
            ct:pal("~p", [Result2]),
            false
    end.

dropped_args(#state{time=Time, queue=Q} = State) ->
    oneof([dropped_args_droptime(State),
           [time(Time), Q],
           [Q]]).

dropped_args_droptime(#state{time=PrevTime, queue=Q}) ->
    ?SUCHTHAT([Time, DropTime, _Q],
              [time(PrevTime), oneof([time(), time(PrevTime)]), Q],
              Time >= DropTime).

dropped_pre(_State, _Args) ->
    true.

dropped_next(#state{time=Time} = State, Value, [Q]) ->
    dropped_next(State, Value, [Time, Q]);
dropped_next(State, Value, [Time, Q]) ->
    dropped_next(State, Value, [Time, Time, Q]);
dropped_next(State, Value, [Time, DropTime, _Q]) ->
    VQ = {call, erlang, element, [3, Value]},
    case prepare_dropped(State, DropTime, Time) of
        {closed, _, NState} ->
            NState#state{queue=VQ};
        {open, _, #state{list=[]} = NState} ->
            NState#state{queue=VQ};
        {open, _, #state{list=NL} = NState} ->
            NState#state{list=droplast(NL), queue=VQ}
    end.

dropped_post(#state{time=Time} = State, [Q], Result) ->
    dropped_post(State, [Time, Q], Result);
dropped_post(State, [Time, Q], Result) ->
    dropped_post(State, [Time, Time, Q], Result);
dropped_post(State, [Time, DropTime, _Q], {Result, Drops, NQ}) ->
    case prepare_dropped(State, DropTime, Time) of
        {closed, Drops, _} ->
            Result =:= closed andalso svalve:is_queue(NQ);
        {open, Drops, #state{list=[]}} ->
            Result =:= empty andalso svalve:is_queue(NQ);
        {open, Drops, #state{list=NL}} ->
            lists:last(NL) =:= Result andalso svalve:is_queue(NQ);
        _ ->
            false
    end.

dropped_r_args(State) ->
    dropped_args(State).

dropped_r_pre(State, Args) ->
    dropped_pre(State, Args).

dropped_r_next(#state{time=Time} = State, Value, [Q]) ->
    dropped_r_next(State, Value, [Time, Q]);
dropped_r_next(State, Value, [Time, Q]) ->
    dropped_r_next(State, Value, [Time, Time, Q]);
dropped_r_next(State, Value, [Time, DropTime, _Q]) ->
    VQ = {call, erlang, element, [3, Value]},
    case prepare_dropped(State, DropTime, Time) of
        {closed, _, NState} ->
            NState#state{queue=VQ};
        {open, _, #state{list=[]} = NState} ->
            NState#state{queue=VQ};
        {open, _, #state{list=NL} = NState} ->
            NState#state{list=droplast(NL), queue=VQ}
    end.

dropped_r_post(#state{time=Time} = State, [Q], Result) ->
    dropped_r_post(State, [Time, Q], Result);
dropped_r_post(State, [Time, Q], Result) ->
    dropped_r_post(State, [Time, Time, Q], Result);
dropped_r_post(State, [Time, DropTime, _Q], {Result, Drops, NQ}) ->
    case prepare_dropped(State, DropTime, Time) of
        {closed, Drops, _} ->
            Result =:= closed andalso svalve:is_queue(NQ);
        {open, Drops, #state{list=[]}} ->
            Result =:= empty andalso svalve:is_queue(NQ);
        {open, Drops, #state{list=NL}} ->
            lists:last(NL) =:= Result andalso svalve:is_queue(NQ);
        _ ->
            false
    end.

open_args(#state{queue=Q}) ->
    [Q].

open_pre(_State, _Args) ->
    true.

open_next(State, VQ, _Args) ->
    State#state{status=open, queue=VQ}.

open_post(_, _Args, Result) ->
    svalve:is_queue(Result).

close_args(#state{queue=Q}) ->
    [Q].

close_pre(_State, _Args) ->
    true.

close_next(State, VQ, _Args) ->
    State#state{status=closed, queue=VQ}.

close_post(_, _Args, Result) ->
    svalve:is_queue(Result).

%% Helpers

advance_time_state(State, Time) ->
    {_, NState} = advance_time(State, Time),
    NState.

advance_time_drops(State, Time) ->
    {Drops, _} = advance_time(State, Time),
    Drops.

advance_time(#state{list=L, time=Time1} = State, Time2) ->
    NTime = max(Time1, Time2),
    Diff = NTime - Time1,
    NL = [{SojournTime + Diff, Item} || {SojournTime, Item} <- L],
    advance_time(State#state{list=NL, time=NTime}).

advance_time(#state{list=[]} = State) ->
    {[], State};
advance_time(#state{squeue_state=[], squeue_drops=[]} = State) ->
    {[], State};
advance_time(#state{squeue_state=[], squeue_drops=SDrops} = State) ->
    advance_time(State#state{squeue_state=SDrops});
advance_time(#state{list=L, squeue_state=[Drop | NSState]} = State) ->
    Drop2 = min(length(L), Drop),
    {Drops, NL} = lists:split(Drop2, L),
    {Drops, State#state{squeue_state=NSState, list=NL}}.

prepare_sojourn(State, OutTime, SojournTime, Time) ->
    {Drops, NState} = advance_time(State, Time),
    {Result, NState2} = prepare_sojourn(NState, OutTime, SojournTime),
    {Result, Drops, NState2}.

prepare_sojourn(#state{status=closed, manager_time=MTime, manager=Manager,
                       manager_state=ManState} = State, OutTime, SojournTime) ->
    NMTime = max(MTime, OutTime),
    {_, NManState} = Manager:handle_sojourn(NMTime, SojournTime, [], ManState),
    {closed, State#state{manager_time=NMTime, manager_state=NManState}};
prepare_sojourn(#state{status=open, manager_time=MTime, list=L, manager=Manager,
                       manager_state=ManState} = State, OutTime, SojournTime) ->
    NMTime = max(MTime, OutTime),
    {Result, NManState} = Manager:handle_sojourn(NMTime, SojournTime, L,
                                                 ManState),
    {Result, State#state{manager_time=NMTime, manager_state=NManState}}.

prepare_dropped(State, DropTime, Time) ->
    {Drops, NState} = advance_time(State, Time),
    {Result, NState2} = prepare_dropped(NState, DropTime),
    {Result, Drops, NState2}.

prepare_dropped(#state{status=closed, manager_time=MTime, manager=Manager,
                       manager_state=ManState} = State, DropTime) ->
    NMTime = max(MTime, DropTime),
    {_, NManState} = Manager:handle_dropped(NMTime, [], ManState),
    {closed, State#state{manager_time=NMTime, manager_state=NManState}};
prepare_dropped(#state{status=open, manager_time=MTime, list=L, manager=Manager,
                       manager_state=ManState} = State, DropTime) ->
    NMTime = max(MTime, DropTime),
    {Result, NManState} = Manager:handle_dropped(NMTime, L, ManState),
    {Result, State#state{manager_time=NMTime, manager_state=NManState}}.

droplast(List) ->
    [_ | Rest] = lists:reverse(List),
    lists:reverse(Rest).
