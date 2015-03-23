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

-export([initial_state/2]).
-export([initial_state/3]).
-export([command/1]).
-export([precondition/2]).
-export([next_state/3]).
-export([postcondition/3]).

-export([new/3]).
-export([new/4]).
-export([new/6]).
-export([new/7]).
-export([join/3]).
-export([filter/4]).
-export([filter/5]).

-record(state, {mod, managers=[], manager, manager_state, feedback,
                feedback_state, status=open, time=0, list=[], queue}).

initial_state(Mod, Manager) ->
    #state{mod=Mod, manager=Manager}.

initial_state(Mod, Feedback, Managers) ->
    #state{mod=Mod, managers=Managers, feedback=Feedback}.

command(#state{queue=undefined} = State) ->
    {call, ?MODULE, new, new_args(State)};
command(#state{mod=Mod, feedback=undefined} = State) ->
    frequency([{20, {call, Mod, in, in_args(State)}},
               {10, {call, Mod, out, out_args(State)}},
               {10, {call, Mod, out_r, out_r_args(State)}},
               {6, {call, ?MODULE, join, join_args(State)}},
               {4, {call, ?MODULE, filter, filter_args(State)}},
               {1, {call, Mod, time, time_args(State)}},
               {1, {call, Mod, timeout, timeout_args(State)}},
               {1, {call, Mod, len, len_args(State)}},
               {1, {call, Mod, to_list, to_list_args(State)}}]);
command(#state{mod=Mod} = State) ->
    frequency([{30, {call, Mod, in, in_args(State)}},
               {15, {call, Mod, sojourn, sojourn_args(State)}},
               {15, {call, Mod, sojourn_r, sojourn_r_args(State)}},
               {6, {call, Mod, out, out_args(State)}},
               {6, {call, Mod, out_r, out_r_args(State)}},
               {6, {call, ?MODULE, join, join_args(State)}},
               {4, {call, Mod, dropped, dropped_args(State)}},
               {4, {call, Mod, dropped_r, dropped_r_args(State)}},
               {4, {call, ?MODULE, filter, filter_args(State)}},
               {4, {call, Mod, open, open_args(State)}},
               {4, {call, Mod, close, close_args(State)}},
               {1, {call, Mod, time, time_args(State)}},
               {1, {call, Mod, timeout, timeout_args(State)}},
               {1, {call, Mod, len, len_args(State)}},
               {1, {call, Mod, to_list, to_list_args(State)}}]).

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
precondition(#state{feedback=undefined}, _) ->
    false;
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
    choose(0, 10).

item() ->
    oneof([a, b, c]).

time(Time) ->
    frequency([{10, ?LET(Incr, choose(0, 3), Time + Incr)},
               {1, choose(0, Time)}]).

new(Mod, Manager, Args) ->
    Mod:new(Manager, Args).

new(Mod, Time, Manager, Args) ->
    Mod:new(Time, Manager, Args).

new(Mod, _Manager, ManMod, ManArgs, Feedback, FbArgs) ->
    S = squeue:new(ManMod, ManArgs),
    Mod:squeue(S, Mod:new(Feedback, FbArgs)).

new(Mod, _Manager, Time, ManMod, ManArgs, Feedback, FbArgs) ->
    S = squeue:new(Time, ManMod, ManArgs),
    Mod:squeue(S, Mod:new(Time, Feedback, FbArgs)).


new_args(#state{mod=Mod, manager=Manager, feedback=undefined}) ->
    Args = [Manager:module(), Manager:args()],
    oneof([[Mod, time() | Args], [Mod | Args]]);
new_args(#state{mod=Mod, managers=Managers, feedback=Feedback}) ->
    ?LET(Manager, elements(Managers),
         begin
             Args = [Manager:module(), Manager:args(),
                     Feedback:module(), Feedback:args()],
             oneof([[Mod, Manager, time() | Args], [Mod, Manager | Args]])
         end).

new_pre(#state{queue=Q}, _Args) ->
    Q =:= undefined.

new_next(State, Value, [Mod, ManMod, ManArgs]) ->
    new_next(State, Value, [Mod, 0, ManMod, ManArgs]);
new_next(#state{manager=Manager} = State, VQ, [_Mod, Time, _ManMod, ManArgs]) ->
    State#state{time=Time, queue=VQ, list=[],
                manager_state=Manager:init(ManArgs)};
new_next(State, Value, [Mod, Manager, ManMod, ManArgs, FbMod, FbArgs]) ->
    new_next(State, Value, [Mod, Manager, 0, ManMod, ManArgs, FbMod, FbArgs]);
new_next(#state{feedback=Feedback} = State, VQ,
         [_Mod, Manager, Time, _ManMod, ManArgs, _FbMod, FbArgs]) ->
    State#state{time=Time, queue=VQ, list=[],
                manager=Manager,
                manager_state=Manager:init(ManArgs),
                feedback_state=Feedback:init(FbArgs)}.

new_post(#state{mod=Mod}, _Args, Q) ->
    Mod:is_queue(Q).

in_args(#state{time=Time, queue=Q}) ->
    oneof([[time(Time), item(), Q],
           [item(), Q]]).

in_pre(_State, _Args) ->
    true.

in_next(#state{time=Time} = State, Value, [Item, Q]) ->
    in_next(State, Value, [Time, Item, Q]);
in_next(State, Value, [Time, Item, _Q]) ->
    VQ = {call, erlang, element, [2, Value]},
    #state{list=L} = NState = advance_time_state(State, Time),
    NState#state{list=L ++ [{0, Item}], queue=VQ}.

in_post(#state{time=Time} = State, [Item, Q], Result) ->
    in_post(State, [Time, Item, Q], Result);
in_post(#state{mod=Mod} = State, [Time, _Item, _Q], {Drops, NQ}) ->
    advance_time_drops(State, Time) =:= Drops andalso Mod:is_queue(NQ).

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
out_post(#state{mod=Mod} = State, [Time, _Q], {Result, Drops, NQ}) ->
    case prepare_out(State, Time) of
        {Drops, #state{list=[]}} ->
            Result =:= empty andalso Mod:is_queue(NQ);
        {Drops, #state{list=[Result|_]}} ->
            Mod:is_queue(NQ);
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
out_r_post(#state{mod=Mod} = State, [Time, _Q], {Result, Drops, NQ}) ->
    case prepare_out_r(State, Time) of
        {Drops, #state{list=[]}} ->
            Result =:= empty andalso Mod:is_queue(NQ);
        {Drops, #state{list=L}} ->
            Result =:= lists:last(L) andalso Mod:is_queue(NQ);
        _ ->
            false
    end.

join(Mod, Q1, {Time2, StartList, Module, Args}) ->
    Q2 = Mod:from_start_list(Time2, StartList, Module, Args),
    Mod:join(Q1, Q2).

join_list(MinStart, MaxStart) ->
    ?LET(Unordered, list(join_item(MinStart, MaxStart)),
         lists:sort(fun start_sort/2, Unordered)).

start_sort({T1, _Item1}, {T2, _Item2}) ->
    T1 =< T2.

join_item(MinStart, MaxStart) ->
    {choose(MinStart, MaxStart), item()}.

join_queue(Time, MinStart, Manager) ->
    ?LET({JoinList, Args}, {join_list(MinStart, Time), Manager:args()},
         {Time, JoinList, Manager:module(), Args}).

join_args(#state{mod=Mod, manager=Manager, list=L, time=Time, queue=Q}) ->
    %% Add default item that has been in queue whole time so that empty queue
    %% has MinStart of 0.
    {SojournTime, _} = lists:last([{Time, a} | L]),
    MinStart = Time-SojournTime,
    [Mod, Q, join_queue(Time, MinStart, Manager)].

join_pre(#state{time=Time, list=L1}, [_S1, {Time, StartList, _, _}]) ->
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


join_next(#state{time=Time, list=L1} = State, VQ,
          [_Q1, {Time, StartList, _Module2, _Args2}]) ->
    L2 = [{Time - Start, Item} || {Start, Item} <- StartList],
    State#state{list=L1++L2, queue=VQ}.

join_post(#state{mod=Mod}, [_Q1, _Q2], NQ) ->
    Mod:is_queue(NQ).

filter(Mod, Method, Item, Q) ->
    Mod:filter(make_filter(Method, Item), Q).

filter(Mod, Time, Method, Item, Q) ->
    Mod:filter(Time, make_filter(Method, Item), Q).

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

filter_args(#state{mod=Mod, time=Time, queue=Q}) ->
    Args = [oneof([duplicate, filter]), item(), Q],
    oneof([[Mod, time(Time) | Args], [Mod | Args]]).

filter_pre(_State, _Args) ->
    true.

filter_next(#state{time=Time} = State, Value, [Mod, Action, Item, Q]) ->
    filter_next(State, Value, [Mod, Time, Action, Item, Q]);
filter_next(State, Value, [_Mod, Time, duplicate, Item, _Q]) ->
    VQ = {call, erlang, element, [2, Value]},
    #state{list=L} = NState = advance_time_state(State, Time),
    Duplicate = fun({_, Item2} = Elem) when Item2 =:= Item ->
                        [Elem, Elem];
                   (Other) ->
                        [Other]
                end,
    NL = lists:flatmap(Duplicate, L),
    NState#state{list=NL, queue=VQ};
filter_next(State, Value, [_Mod, Time, filter, Item, _Q]) ->
    VQ = {call, erlang, element, [2, Value]},
    #state{list=L} = NState = advance_time_state(State, Time),
    Filter = fun({_, Item2} ) -> Item2 =:= Item end,
    NL = lists:filter(Filter, L),
    NState#state{list=NL, queue=VQ}.

filter_post(#state{time=Time} = State, [Mod, Action, Item, Q], Result) ->
    filter_post(State, [Mod, Time, Action, Item, Q], Result);
filter_post(State, [Mod, Time, _Action, _Item, _Q], {Drops, NQ}) ->
    advance_time_drops(State, Time) =:= Drops andalso Mod:is_queue(NQ).

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

timeout_post(#state{mod=Mod} = State, [Time, _Q], {Drops, NQ}) ->
    advance_time_drops(State, Time) =:= Drops andalso Mod:is_queue(NQ).

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

sojourn_args(#state{time=Time, queue=Q}) ->
    oneof([[time(Time), sojourn(), Q],
           [sojourn(), Q]]).

sojourn_pre(_State, _Args) ->
    true.

sojourn_next(#state{time=Time} = State, Value, [SojournTime, Q]) ->
    sojourn_next(State, Value, [Time, SojournTime, Q]);
sojourn_next(State, Value, [Time, SojournTime, _Q]) ->
    VQ = {call, erlang, element, [3, Value]},
    case prepare_sojourn(State, handle_sojourn, SojournTime, Time) of
        {closed, _, NState} ->
            NState#state{queue=VQ};
        {open, _, #state{list=[]} = NState} ->
            NState#state{queue=VQ};
        {open, _, #state{list=[_Out | NL]} = NState} ->
            NState#state{list=NL, queue=VQ}
    end.

sojourn_post(#state{time=Time} = State, [SojournTime, Q], Result) ->
    sojourn_post(State, [Time, SojournTime, Q], Result);
sojourn_post(#state{mod=Mod} = State, [Time, SojournTime, _Q],
         {Result, Drops, NQ}) ->
    case prepare_sojourn(State, handle_sojourn, SojournTime, Time) of
        {closed, Drops, _} ->
            Result =:= closed andalso Mod:is_queue(NQ);
        {open, Drops, #state{list=[]}} ->
            Result =:= empty andalso Mod:is_queue(NQ);
        {open, Drops, #state{list=[Result|_]}} ->
            Mod:is_queue(NQ);
        _ ->
            false
    end.

sojourn_r_args(State) ->
    sojourn_args(State).

sojourn_r_pre(State, Args) ->
    sojourn_pre(State, Args).

sojourn_r_next(#state{time=Time} = State, Value, [SojournTime, Q]) ->
    sojourn_r_next(State, Value, [Time, SojournTime, Q]);
sojourn_r_next(State, Value, [Time, SojournTime, _Q]) ->
    VQ = {call, erlang, element, [3, Value]},
    case prepare_sojourn(State, handle_sojourn_r, SojournTime, Time) of
        {closed, _, NState} ->
            NState#state{queue=VQ};
        {open, _, #state{list=[]} = NState} ->
            NState#state{queue=VQ};
        {open, _, #state{list=NL} = NState} ->
            NState#state{list=droplast(NL), queue=VQ}
    end.

sojourn_r_post(#state{time=Time} = State, [SojournTime, Q], Result) ->
    sojourn_r_post(State, [Time, SojournTime, Q], Result);
sojourn_r_post(#state{mod=Mod} = State, [Time, SojournTime, _Q],
         {Result, Drops, NQ}) ->
    case prepare_sojourn(State, handle_sojourn_r, SojournTime, Time) of
        {closed, Drops, _} ->
            Result =:= closed andalso Mod:is_queue(NQ);
        {open, Drops, #state{list=[]}} ->
            Result =:= empty andalso Mod:is_queue(NQ);
        {open, Drops, #state{list=NL}} ->
            lists:last(NL) =:= Result andalso Mod:is_queue(NQ);
        _ ->
            false
    end.


dropped_args(#state{time=Time, queue=Q}) ->
    oneof([[time(Time), Q],
           [Q]]).

dropped_pre(_State, _Args) ->
    true.

dropped_next(#state{time=Time} = State, Value, [Q]) ->
    dropped_next(State, Value, [Time, Q]);
dropped_next(State, Value, [Time, _Q]) ->
    VQ = {call, erlang, element, [3, Value]},
    case prepare_dropped(State, handle_dropped, Time) of
        {closed, _, NState} ->
            NState#state{queue=VQ};
        {open, _, #state{list=[]} = NState} ->
            NState#state{queue=VQ};
        {open, _, #state{list=NL} = NState} ->
            NState#state{list=droplast(NL), queue=VQ}
    end.

dropped_post(#state{time=Time} = State, [Q], Result) ->
    dropped_post(State, [Time, Q], Result);
dropped_post(#state{mod=Mod} = State, [Time, _Q],
         {Result, Drops, NQ}) ->
    case prepare_dropped(State, handle_dropped, Time) of
        {closed, Drops, _} ->
            Result =:= closed andalso Mod:is_queue(NQ);
        {open, Drops, #state{list=[]}} ->
            Result =:= empty andalso Mod:is_queue(NQ);
        {open, Drops, #state{list=NL}} ->
            lists:last(NL) =:= Result andalso Mod:is_queue(NQ);
        _ ->
            false
    end.

dropped_r_args(State) ->
    dropped_args(State).

dropped_r_pre(State, Args) ->
    dropped_pre(State, Args).

dropped_r_next(#state{time=Time} = State, Value, [Q]) ->
    dropped_r_next(State, Value, [Time, Q]);
dropped_r_next(State, Value, [Time, _Q]) ->
    VQ = {call, erlang, element, [3, Value]},
    case prepare_dropped(State, handle_dropped_r, Time) of
        {closed, _, NState} ->
            NState#state{queue=VQ};
        {open, _, #state{list=[]} = NState} ->
            NState#state{queue=VQ};
        {open, _, #state{list=NL} = NState} ->
            NState#state{list=droplast(NL), queue=VQ}
    end.

dropped_r_post(#state{time=Time} = State, [Q], Result) ->
    dropped_r_post(State, [Time, Q], Result);
dropped_r_post(#state{mod=Mod} = State, [Time, _Q],
         {Result, Drops, NQ}) ->
    case prepare_dropped(State, handle_dropped_r, Time) of
        {closed, Drops, _} ->
            Result =:= closed andalso Mod:is_queue(NQ);
        {open, Drops, #state{list=[]}} ->
            Result =:= empty andalso Mod:is_queue(NQ);
        {open, Drops, #state{list=NL}} ->
            lists:last(NL) =:= Result andalso Mod:is_queue(NQ);
        _ ->
            false
    end.

open_args(#state{queue=Q}) ->
    [Q].

open_pre(_State, _Args) ->
    true.

open_next(State, VQ, _Args) ->
    State#state{status=open, queue=VQ}.

open_post(#state{mod=Mod}, _Args, Result) ->
    Mod:is_queue(Result).

close_args(#state{queue=Q}) ->
    [Q].

close_pre(_State, _Args) ->
    true.

close_next(State, VQ, _Args) ->
    State#state{status=closed, queue=VQ}.

close_post(#state{mod=Mod}, _Args, Result) ->
    Mod:is_queue(Result).

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

prepare_sojourn(#state{time=Time} = State, Fun, SojournTime, NTime)
  when Time > NTime ->
    prepare_sojourn(State, Fun, SojournTime, Time);
prepare_sojourn(#state{status=closed} = State, Fun, SojournTime, NTime)
  when Fun =/= handle_sojourn_closed ->
    prepare_sojourn(State, handle_sojourn_closed, SojournTime, NTime);
prepare_sojourn(#state{feedback=Feedback, feedback_state=FbState,
                       manager=Manager, manager_state=ManState, time=Time,
                       list=L} = State, Fun, SojournTime, NTime) ->
    Diff = NTime - Time,
    NL = [{ItemSojournTime + Diff, Item} || {ItemSojournTime, Item} <- L],
    {SojournTimes, _} = lists:unzip(NL),
    {Result, DropCount, NManState, NFbState} =
        Feedback:Fun(NTime, SojournTime, SojournTimes, Manager, ManState,
                     FbState),
    {Drops, NL2} = lists:split(DropCount, NL),
    {Result, Drops, State#state{time=NTime, list=NL2, manager_state=NManState,
                                feedback_state=NFbState}}.

prepare_dropped(#state{time=Time} = State, Fun, NTime) when Time > NTime ->
    prepare_dropped(State, Fun, Time);
prepare_dropped(#state{status=closed} = State, Fun, NTime)
  when Fun =/= handle_dropped_closed ->
    prepare_dropped(State, handle_dropped_closed, NTime);
prepare_dropped(#state{feedback=Feedback, feedback_state=FbState,
                       manager=Manager, manager_state=ManState, time=Time,
                       list=L} = State, Fun, NTime) ->
    Diff = NTime - Time,
    NL = [{ItemSojournTime + Diff, Item} || {ItemSojournTime, Item} <- L],
    {SojournTimes, _} = lists:unzip(NL),
    {Result, DropCount, NManState, NFbState} =
        Feedback:Fun(NTime, SojournTimes, Manager, ManState, FbState),
    {Drops, NL2} = lists:split(DropCount, NL),
    {Result, Drops, State#state{time=NTime, list=NL2, manager_state=NManState,
                                feedback_state=NFbState}}.

droplast(List) ->
    [_ | Rest] = lists:reverse(List),
    lists:reverse(Rest).
