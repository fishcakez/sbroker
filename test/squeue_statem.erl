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
-module(squeue_statem).

-include_lib("proper/include/proper.hrl").

-compile({no_auto_import, [time/0]}).

-export([initial_state/1]).
-export([command/1]).
-export([precondition/2]).
-export([next_state/3]).
-export([postcondition/3]).

-export([new/2]).
-export([new/3]).
-export([drop/1]).
-export([drop_r/1]).
-export([join/2]).
-export([filter/3]).
-export([filter/4]).

-record(state, {manager, manager_state, time=0, tail_time=0, list=[], queue}).

initial_state(Manager) ->
    #state{manager=Manager}.

command(#state{queue=undefined} = State) ->
    {call, ?MODULE, new, new_args(State)};
command(State) ->
    frequency([{20, {call, squeue, in, in_args(State)}},
               {10, {call, squeue, out, out_args(State)}},
               {10, {call, squeue, out_r, out_r_args(State)}},
               {6, {call, ?MODULE, drop, drop_args(State)}},
               {6, {call, ?MODULE, drop_r, drop_r_args(State)}},
               {6, {call, ?MODULE, join, join_args(State)}},
               {4, {call, ?MODULE, filter, filter_args(State)}},
               {1, {call, squeue, time, time_args(State)}},
               {1, {call, squeue, timeout, timeout_args(State)}},
               {1, {call, squeue, len, len_args(State)}},
               {1, {call, squeue, to_list, to_list_args(State)}}]).

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
    to_list_pre(State, Args).

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
    to_list_next(State, Value, Args).

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
    to_list_post(State, Args, Result).

time() ->
    choose(-10, 10).

item() ->
    oneof([a, b, c]).

time(Time) ->
    frequency([{10, ?LET(Incr, choose(0, 3), Time + Incr)},
               {1, choose(-10, Time)}]).

new(Manager, Args) ->
    squeue:new(Manager, Args).

new(Time, Manager, Args) ->
    squeue:new(Time, Manager, Args).

new_args(#state{manager=Manager}) ->
    Args = [Manager:module(), Manager:args()],
    oneof([[time() | Args], Args]).

new_pre(#state{queue=Q}, _Args) ->
    Q =:= undefined.

new_next(State, Value, [Module, Args]) ->
    new_next(State, Value, [0, Module, Args]);
new_next(#state{manager=Manager} = State, VQ, [Time, _, Args]) ->
    State#state{time=Time, tail_time=Time, queue=VQ, list=[],
                manager_state=Manager:init(Args)}.

new_post(_, _Args, Q) ->
    squeue:is_queue(Q).

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
    advance_time_drops(State, Time) =:= Drops andalso squeue:is_queue(NQ).

out_args(#state{time=Time, queue=Q}) ->
    oneof([[time(Time), Q],
           [Q]]).

out_pre(_State, _Args) ->
    true.

out_next(#state{time=Time} = State, Value, [Q]) ->
    out_next(State, Value, [Time, Q]);
out_next(State, Value, [Time, _Q]) ->
    VQ = {call, erlang, element, [3, Value]},
    case prepare_out_state(State, Time) of
        #state{list=[]} = NState ->
            NState#state{queue=VQ};
        #state{list=[_Out | NL]} = NState ->
            NState#state{list=NL, queue=VQ}
    end.

out_post(#state{time=Time} = State, [Q], Result) ->
    out_post(State, [Time, Q], Result);
out_post(State, [Time, _Q], {Result, Drops, NQ}) ->
    case prepare_out(State, Time) of
        {Drops, #state{list=[]}} ->
            Result =:= empty andalso squeue:is_queue(NQ);
        {Drops, #state{list=[Result|_]}} ->
            squeue:is_queue(NQ);
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
    case prepare_out_r_state(State, Time) of
        #state{list=[]} = NState ->
            NState#state{queue=VQ};
        #state{list=L} = NState ->
            NState#state{list=droplast(L), queue=VQ}
    end.

out_r_post(#state{time=Time} = State, [Q], Result) ->
    out_r_post(State, [Time, Q], Result);
out_r_post(State, [Time, _Q], {Result, Drops, NQ}) ->
    case prepare_out_r(State, Time) of
        {Drops, #state{list=[]}} ->
            Result =:= empty andalso squeue:is_queue(NQ);
        {Drops, #state{list=L}} ->
            Result =:= lists:last(L) andalso squeue:is_queue(NQ);
        _ ->
            false
    end.

drop(Args) ->
    try apply(squeue, drop, Args) of
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
            Drops =:= [Drop] andalso squeue:is_queue(NQ);
        {Drops, _} ->
            squeue:is_queue(NQ);
        _ ->
            false
    end.

drop_r(Args) ->
    try apply(squeue, drop_r, Args) of
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
            Drops =:= [lists:last(L)] andalso squeue:is_queue(NQ);
        {Drops, _} ->
            squeue:is_queue(NQ);
        _ ->
            false
    end.

join(Q1, {Time2, TailTime, StartList, Module, Args}) ->
    Q2 = squeue:from_start_list(Time2, TailTime, StartList, Module, Args),
    squeue:join(Q1, Q2).

join_list(MinStart, MaxStart) ->
    ?LET(Unordered, list(join_item(MinStart, MaxStart)),
         lists:sort(fun start_sort/2, Unordered)).

start_sort({T1, _Item1}, {T2, _Item2}) ->
    T1 =< T2.

join_item(MinStart, MaxStart) ->
    {choose(MinStart, MaxStart), item()}.

join_queue(Time, MinStart, Manager) ->
    ?LET({JoinList, Args}, {join_list(MinStart, Time), Manager:args()},
         begin
             {MaxItemTime, _} = lists:last([{-10, a} | JoinList]),
             TailTime = choose(MaxItemTime, Time),
             {Time, TailTime, JoinList, Manager:module(), Args}
         end).

join_args(#state{manager=Manager, list=L, time=Time, queue=Q}) ->
    %% Add default item that has been in queue whole time so that empty queue
    %% has MinStart of -10.
    {SojournTime, _} = lists:last([{Time+10, a} | L]),
    MinStart = Time-SojournTime,
    [Q, join_queue(Time, MinStart, Manager)].

join_pre(#state{time=Time, list=L1},
         [_Q1, _, {Time, _, StartList, _, _} | _]) ->
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


join_next(#state{time=Time, tail_time=TailTime, list=L1} = State, VQ,
          [_Q1, {Time, TailTime2, StartList, _Module2, _Args2}]) ->
    L2 = [{Time - Start, Item} || {Start, Item} <- StartList],
    State#state{tail_time=max(TailTime, TailTime2), list=L1++L2, queue=VQ}.

join_post(_, _, NQ) ->
    squeue:is_queue(NQ).

filter(Method, Item, Q) ->
    squeue:filter(make_filter(Method, Item), Q).

filter(Time, Method, Item, Q) ->
    squeue:filter(Time, make_filter(Method, Item), Q).

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
    advance_time_drops(State, Time) =:= Drops andalso squeue:is_queue(NQ).

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
    advance_time_drops(State, Time) =:= Drops andalso squeue:is_queue(NQ).

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

%% Helpers

advance_time_state(State, Time) ->
    {_, NState} = advance_time(State, Time),
    NState.

advance_time_drops(State, Time) ->
    {Drops, _} = advance_time(State, Time),
    Drops.

advance_time(State, Time) ->
    prepare(State, handle_timeout, Time).

prepare_out_state(State, Time) ->
    {_, NState} = prepare_out(State, Time),
    NState.

prepare_out(State, Time) ->
    prepare(State, handle_out, Time).

prepare_out_r_state(State, Time) ->
    {_, NState} = prepare_out_r(State, Time),
    NState.

prepare_out_r(State, Time) ->
    prepare(State, handle_out_r, Time).

prepare(#state{time=Time} = State, Fun, NTime) when Time > NTime ->
    prepare(State, Fun, Time);
prepare(#state{manager=Manager, manager_state=ManState, time=Time,
               list=L} = State, Fun, NTime) ->
    Diff = NTime - Time,
    NL = [{SojournTime + Diff, Item} || {SojournTime, Item} <- L],
    {SojournTimes, _} = lists:unzip(NL),
    {DropCount, NManState} = Manager:Fun(NTime, SojournTimes, ManState),
    {Drops, NL2} = lists:split(DropCount, NL),
    {Drops, State#state{time=NTime, list=NL2, manager_state=NManState}}.

droplast(List) ->
    [_ | Rest] = lists:reverse(List),
    lists:reverse(Rest).
