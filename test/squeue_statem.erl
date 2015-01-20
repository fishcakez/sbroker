-module(squeue_statem).

-include_lib("proper/include/proper.hrl").

-compile({no_auto_import, [time/0]}).

-export([initial_state/1]).
-export([command/1]).
-export([precondition/2]).
-export([next_state/3]).
-export([postcondition/3]).

-export([join/2]).
-export([filter/3]).
-export([filter/4]).

-record(state, {mod, mod_state, time=0, list=[], squeue}).

initial_state(Mod) ->
    #state{mod=Mod}.

command(#state{squeue=undefined} = State) ->
    {call, squeue, new, new_args(State)};
command(State) ->
    frequency([{20, {call, squeue, in, in_args(State)}},
               {10, {call, squeue, out, out_args(State)}},
               {10, {call, squeue, out_r, out_r_args(State)}},
               {6, {call, ?MODULE, join, join_args(State)}},
               {4, {call, ?MODULE, filter, filter_args(State)}},
               {1, {call, squeue, time, time_args(State)}},
               {1, {call, squeue, timeout, timeout_args(State)}},
               {1, {call, squeue, len, len_args(State)}},
               {1, {call, squeue, to_list, to_list_args(State)}}]).

precondition(State, {call, _, new, Args}) ->
    new_pre(State, Args);
precondition(#state{squeue=undefined}, _) ->
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
    to_list_pre(State, Args).

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
    to_list_next(State, Value, Args).

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
    to_list_post(State, Args, Result).

time() ->
    choose(0, 10).

item() ->
    oneof([a, b, c]).

time(Time) ->
    frequency([{10, ?LET(Incr, choose(0, 3), Time + Incr)},
               {1, choose(0, Time)}]).

new_args(#state{mod=Mod}) ->
    Args = [Mod:module(), Mod:args()],
    oneof([[time() | Args], Args]).

new_pre(#state{squeue=S}, _Args) ->
    S =:= undefined.

new_next(State, Value, [SMod, Args]) ->
    new_next(State, Value, [0, SMod, Args]);
new_next(#state{mod=Mod} = State, VS, [Time, _SMod, Args]) ->
    State#state{time=Time, squeue=VS, list=[], mod_state=Mod:init(Args)}.

new_post(_State, _Args, S) ->
    squeue:is_queue(S).

in_args(#state{time=Time, squeue=S}) ->
    oneof([[time(Time), item(), S],
           [item(), S]]).

in_pre(_State, _Args) ->
    true.

in_next(#state{time=Time} = State, Value, [Item, S]) ->
    in_next(State, Value, [Time, Item, S]);
in_next(State, Value, [Time, Item, _S]) ->
    VS = {call, erlang, element, [2, Value]},
    #state{list=L} = NState = advance_time_state(State, Time),
    NState#state{list=L ++ [{0, Item}], squeue=VS}.

in_post(#state{time=Time} = State, [Item, S], Result) ->
    in_post(State, [Time, Item, S], Result);
in_post(State, [Time, _Item, _S], {Drops, NS}) ->
    advance_time_drops(State, Time) =:= Drops andalso squeue:is_queue(NS).

out_args(#state{time=Time, squeue=S}) ->
    oneof([[time(Time), S],
           [S]]).

out_pre(_State, _Args) ->
    true.

out_next(#state{time=Time} = State, Value, [S]) ->
    out_next(State, Value, [Time, S]);
out_next(State, Value, [Time, _S]) ->
    VS = {call, erlang, element, [3, Value]},
    NState = advance_time_state(State, Time),
    case prepare_out_state(NState) of
        #state{list=[]} = NState2 ->
            NState2#state{squeue=VS};
        #state{list=[_Out | NL]} = NState2 ->
            NState2#state{list=NL, squeue=VS}
    end.

out_post(#state{time=Time} = State, [S], Result) ->
    out_post(State, [Time, S], Result);
out_post(State, [Time, _S], {Result, Drops, NS}) ->
    {Drops2, NState} = advance_time(State, Time),
    case prepare_out(NState) of
        {Drops3, #state{list=[]}} ->
            Result =:= empty andalso Drops2 ++ Drops3 =:= Drops andalso
            squeue:is_queue(NS);
        {Drops3, #state{list=[Result|_]}} ->
            Drops2 ++ Drops3 =:= Drops andalso squeue:is_queue(NS);
        _ ->
            false
    end.

out_r_args(State) ->
    out_args(State).

out_r_pre(State, Args) ->
    out_pre(State, Args).

out_r_next(#state{time=Time} = State, Value, [S]) ->
    out_r_next(State, Value, [Time, S]);
out_r_next(State, Value, [Time, _S]) ->
    VS = {call, erlang, element, [3, Value]},
    NState = advance_time_state(State, Time),
    case prepare_out_state(NState) of
        #state{list=[]} = NState2 ->
            NState2#state{squeue=VS};
        #state{list=L} = NState2 ->
            NState2#state{list=droplast(L), squeue=VS}
    end.

out_r_post(#state{time=Time} = State, [S], Result) ->
    out_r_post(State, [Time, S], Result);
out_r_post(State, [Time, _S], {Result, Drops, NS}) ->
    {Drops2, NState} = advance_time(State, Time),
    case prepare_out(NState) of
        {Drops3, #state{list=[]}} ->
            Result =:= empty andalso Drops2 ++ Drops3 =:= Drops andalso
            squeue:is_queue(NS);
        {Drops3, #state{list=L}} ->
            Result =:= lists:last(L) andalso Drops2 ++ Drops3 =:= Drops andalso
            squeue:is_queue(NS)
    end.

join(S1, {Time2, StartList, Module, Args}) ->
    S2 = squeue:from_start_list(Time2, StartList, Module, Args),
    squeue:join(S1, S2).

join_list(MinStart, MaxStart) ->
    ?LET(Unordered, list(join_item(MinStart, MaxStart)),
         lists:sort(fun start_sort/2, Unordered)).

start_sort({T1, _Item1}, {T2, _Item2}) ->
    T1 =< T2.

join_item(MinStart, MaxStart) ->
    {choose(MinStart, MaxStart), item()}.

join_queue(Time, MinStart, Mod) ->
    ?LET({JoinList, Args}, {join_list(MinStart, Time), Mod:args()},
         {Time, JoinList, Mod:module(), Args}).

join_args(#state{mod=Mod, list=L, time=Time, squeue=S}) ->
    %% Add default item that has been in queue whole time so that empty queue
    %% has MinStart of 0.
    {SojournTime, _} = lists:last([{Time, a} | L]),
    MinStart = Time-SojournTime,
    [S, join_queue(Time, MinStart, Mod)].

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


join_next(#state{time=Time, list=L1} = State, VS,
          [_S1, {Time, StartList, _Module2, _Args2}]) ->
    L2 = [{Time - Start, Item} || {Start, Item} <- StartList],
    State#state{list=L1++L2, squeue=VS}.

join_post(_State, [_S1, _S2], VS) ->
    squeue:is_queue(VS).

filter(Method, Item, S) ->
    squeue:filter(make_filter(Method, Item), S).

filter(Time, Method, Item, S) ->
    squeue:filter(Time, make_filter(Method, Item), S).

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

filter_args(#state{time=Time, squeue=S}) ->
    Args = [oneof([duplicate, filter]), item(), S],
    oneof([[time(Time) | Args], Args]).

filter_pre(_State, _Args) ->
    true.

filter_next(#state{time=Time} = State, Value, [Action, Item, S]) ->
    filter_next(State, Value, [Time, Action, Item, S]);
filter_next(State, Value, [Time, duplicate, Item, _S]) ->
    VS = {call, erlang, element, [2, Value]},
    NState = advance_time_state(State, Time),
    #state{list=L} = NState2 = prepare_out_state(NState),
    Duplicate = fun({_, Item2} = Elem) when Item2 =:= Item ->
                        [Elem, Elem];
                   (Other) ->
                        [Other]
                end,
    NL = lists:flatmap(Duplicate, L),
    NState2#state{list=NL, squeue=VS};
filter_next(State, Value, [Time, filter, Item, _S]) ->
    VS = {call, erlang, element, [2, Value]},
    NState = advance_time_state(State, Time),
    #state{list=L} = NState2 = prepare_out_state(NState),
    Filter = fun({_, Item2} ) -> Item2 =:= Item end,
    NL = lists:filter(Filter, L),
    NState2#state{list=NL, squeue=VS}.

filter_post(#state{time=Time} = State, [Action, Item, S], Result) ->
    filter_post(State, [Time, Action, Item, S], Result);
filter_post(State, [Time, _Action, _Item, _S], {Drops, NS}) ->
    {Drops2, NState} = advance_time(State, Time),
    Drops3 = prepare_out_drops(NState),
    Drops2 ++ Drops3 =:= Drops andalso squeue:is_queue(NS).

time_args(#state{squeue=S}) ->
    [S].

time_pre(_State, _Args) ->
    true.

time_next(State, _Value, _Args) ->
    State.

time_post(#state{time=Time}, _Args, Time2) ->
    Time =:= Time2.

timeout_args(#state{time=Time, squeue=S}) ->
    [time(Time), S].

timeout_pre(_State, _Args) ->
      true.

timeout_next(State, Value, [Time, _S]) ->
    VS = {call, erlang, element, [2, Value]},
    NState = advance_time_state(State, Time),
    NState#state{squeue=VS}.

timeout_post(State, [Time, _S], {Drops, NS}) ->
    advance_time_drops(State, Time) =:= Drops andalso squeue:is_queue(NS).

len_args(#state{squeue=S}) ->
    [S].

len_pre(_State, _Args) ->
    true.

len_next(State, _Value, _Args) ->
    State.

len_post(#state{list=L}, [_S], Len) ->
    length(L) =:= Len.

to_list_args(#state{squeue=S}) ->
    [S].

to_list_pre(_State, _Args) ->
    true.

to_list_next(State, _Value, _Args) ->
    State.

to_list_post(#state{list=L}, [_S], List) ->
    {_, Items} = lists:unzip(L),
    Items =:= List.

advance_time_state(State, Time) ->
    {_, NState} = advance_time(State, Time),
    NState.

advance_time_drops(State, Time) ->
    {Drops, _} = advance_time(State, Time),
    Drops.

advance_time(#state{time=Time} = State, NTime) when Time > NTime ->
    advance_time(State, Time);
advance_time(#state{mod=Mod, mod_state=ModState, time=Time, list=L} = State,
             NTime) ->
    Diff = NTime - Time,
    NL = [{SojournTime + Diff, Item} || {SojournTime, Item} <- L],
    {SojournTimes, _} = lists:unzip(NL),
    {DropCount, NModState} = Mod:handle_timeout(NTime, SojournTimes, ModState),
    {Drops, NL2} = lists:split(DropCount, NL),
    {Drops, State#state{time=NTime, list=NL2, mod_state=NModState}}.

prepare_out_state(State) ->
    {_, NState} = prepare_out(State),
    NState.

prepare_out_drops(State) ->
    {Drops, _} = prepare_out(State),
    Drops.

prepare_out(#state{mod=Mod, mod_state=ModState, time=Time, list=L} = State) ->
    {SojournTimes, _} = lists:unzip(L),
    {DropCount, NModState} = Mod:handle_out(Time, SojournTimes, ModState),
    {Drops, NL} = lists:split(DropCount, L),
    {Drops, State#state{list=NL, mod_state=NModState}}.

droplast(List) ->
    [_ | Rest] = lists:reverse(List),
    lists:reverse(Rest).
