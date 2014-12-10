-module(squeue_statem).

-include_lib("proper/include/proper.hrl").

-export([initial_state/1]).
-export([command/1]).
-export([precondition/2]).
-export([next_state/3]).
-export([postcondition/3]).

-export([join/2]).
-export([join/3]).
-export([filter/3]).
-export([filter/4]).
-export([get_empty/1]).
-export([get_empty/2]).
-export([get_r_empty/1]).
-export([get_r_empty/2]).
-export([drop_empty/1]).
-export([drop_empty/2]).
-export([drop_r_empty/1]).
-export([drop_r_empty/2]).

-record(state, {mod, mod_state, time=0, list=[], squeue}).

initial_state(Mod) ->
    #state{mod=Mod}.

command(#state{squeue=undefined} = State) ->
    oneof([{call, squeue, new, new_args(State)},
           {call, squeue, from_list, from_list_args(State)}]);
command(State) ->
    frequency([{10, {call, squeue, in, in_args(State)}},
               {10, {call, squeue, in_r, in_r_args(State)}},
               {10, {call, squeue, out, out_args(State)}},
               {10, {call, squeue, out_r, out_r_args(State)}},
               {6, {call, ?MODULE, join, join_args(State)}},
               {4, {call, squeue, reverse, reverse_args(State)}},
               {4, {call, squeue, split, split_args(State)}},
               {4, {call, ?MODULE, filter, filter_args(State)}},
               {3, {call, squeue, get, get_args(State)}},
               {3, {call, ?MODULE, get_empty, get_empty_args(State)}},
               {3, {call, squeue, get_r, get_r_args(State)}},
               {3, {call, ?MODULE, get_r_empty, get_r_empty_args(State)}},
               {3, {call, squeue, peek, peek_args(State)}},
               {3, {call, squeue, peek_r, peek_r_args(State)}},
               {3, {call, squeue, drop, drop_args(State)}},
               {3, {call, ?MODULE, drop_empty, drop_empty_args(State)}},
               {3, {call, squeue, drop_r, drop_r_args(State)}},
               {3, {call, ?MODULE, drop_r_empty, drop_r_empty_args(State)}},
               {1, {call, squeue, cons, in_args(State)}},
               {1, {call, squeue, head, get_args(State)}},
               {1, {call, squeue, tail, drop_args(State)}},
               {1, {call, squeue, snoc, in_r_args(State)}},
               {1, {call, squeue, daeh, get_r_args(State)}},
               {1, {call, squeue, last, get_r_args(State)}},
               {1, {call, squeue, liat, drop_r_args(State)}},
               {1, {call, squeue, init, drop_r_args(State)}},
               {1, {call, squeue, lait, drop_r_args(State)}},
               {1, {call, squeue, time, time_args(State)}},
               {1, {call, squeue, timeout, timeout_args(State)}},
               {1, {call, squeue, is_empty, is_empty_args(State)}},
               {1, {call, squeue, len, len_args(State)}},
               {1, {call, squeue, to_list, to_list_args(State)}},
               {1, {call, squeue, member, member_args(State)}}]).

precondition(State, {call, _, new, Args}) ->
    new_pre(State, Args);
precondition(State, {call, _, from_list, Args}) ->
    from_list_pre(State, Args);
precondition(#state{squeue=undefined}, _) ->
    false;
precondition(State, {call, _, in, Args}) ->
    in_pre(State, Args);
precondition(State, {call, _, in_r, Args}) ->
    in_r_pre(State, Args);
precondition(State, {call, _, out, Args}) ->
    out_pre(State, Args);
precondition(State, {call, _, out_r, Args}) ->
    out_r_pre(State, Args);
precondition(State, {call, _, join, Args}) ->
    join_pre(State, Args);
precondition(State, {call, _, reverse, Args}) ->
    reverse_pre(State, Args);
precondition(State, {call, _, split, Args}) ->
    split_pre(State, Args);
precondition(State, {call, _, filter, Args}) ->
    filter_pre(State, Args);
precondition(State, {call, _, get, Args}) ->
    get_pre(State, Args);
precondition(State, {call, _, get_empty, Args}) ->
    get_empty_pre(State, Args);
precondition(State, {call, _, get_r, Args}) ->
    get_r_pre(State, Args);
precondition(State, {call, _, get_r_empty, Args}) ->
    get_r_empty_pre(State, Args);
precondition(State, {call, _, peek, Args}) ->
    peek_pre(State, Args);
precondition(State, {call, _, peek_r, Args}) ->
    peek_r_pre(State, Args);
precondition(State, {call, _, drop, Args}) ->
    drop_pre(State, Args);
precondition(State, {call, _, drop_empty, Args}) ->
    drop_empty_pre(State, Args);
precondition(State, {call, _, drop_r, Args}) ->
    drop_r_pre(State, Args);
precondition(State, {call, _, drop_r_empty, Args}) ->
    drop_r_empty_pre(State, Args);
precondition(State, {call, _, cons, Args}) ->
    in_pre(State, Args);
precondition(State, {call, _, head, Args}) ->
    get_pre(State, Args);
precondition(State, {call, _, tail, Args}) ->
    drop_pre(State, Args);
precondition(State, {call, _, snoc, Args}) ->
    in_r_pre(State, Args);
precondition(State, {call, _, daeh, Args}) ->
    get_r_pre(State, Args);
precondition(State, {call, _, last, Args}) ->
    get_r_pre(State, Args);
precondition(State, {call, _, liat, Args}) ->
    drop_r_pre(State, Args);
precondition(State, {call, _, init, Args}) ->
    drop_r_pre(State, Args);
precondition(State, {call, _, lait, Args}) ->
    drop_r_pre(State, Args);
precondition(State, {call, _, time, Args}) ->
    time_pre(State, Args);
precondition(State, {call, _, timeout, Args}) ->
    timeout_pre(State, Args);
precondition(State, {call, _, is_empty, Args}) ->
    is_empty_pre(State, Args);
precondition(State, {call, _, len, Args}) ->
    len_pre(State, Args);
precondition(State, {call, _, to_list, Args}) ->
    to_list_pre(State, Args);
precondition(State, {call, _, member, Args}) ->
    member_pre(State, Args).

next_state(State, Value, {call, _, new, Args}) ->
    new_next(State, Value, Args);
next_state(State, Value, {call, _, from_list, Args}) ->
    from_list_next(State, Value, Args);
next_state(State, Value, {call, _, in, Args}) ->
    in_next(State, Value, Args);
next_state(State, Value, {call, _, in_r, Args}) ->
    in_r_next(State, Value, Args);
next_state(State, Value, {call, _, out, Args}) ->
    out_next(State, Value, Args);
next_state(State, Value, {call, _, out_r, Args}) ->
    out_r_next(State, Value, Args);
next_state(State, Value, {call, _, join, Args}) ->
    join_next(State, Value, Args);
next_state(State, Value, {call, _, reverse, Args}) ->
    reverse_next(State, Value, Args);
next_state(State, Value, {call, _, split, Args}) ->
    split_next(State, Value, Args);
next_state(State, Value, {call, _, filter, Args}) ->
    filter_next(State, Value, Args);
next_state(State, Value, {call, _, get, Args}) ->
    get_next(State, Value, Args);
next_state(State, Value, {call, _, get_empty, Args}) ->
    get_empty_next(State, Value, Args);
next_state(State, Value, {call, _, get_r, Args}) ->
    get_r_next(State, Value, Args);
next_state(State, Value, {call, _, get_r_empty, Args}) ->
    get_r_empty_next(State, Value, Args);
next_state(State, Value, {call, _, peek, Args}) ->
    peek_next(State, Value, Args);
next_state(State, Value, {call, _, peek_r, Args}) ->
    peek_r_next(State, Value, Args);
next_state(State, Value, {call, _, drop, Args}) ->
    drop_next(State, Value, Args);
next_state(State, Value, {call, _, drop_empty, Args}) ->
    drop_empty_next(State, Value, Args);
next_state(State, Value, {call, _, drop_r, Args}) ->
    drop_r_next(State, Value, Args);
next_state(State, Value, {call, _, drop_r_empty, Args}) ->
    drop_r_empty_next(State, Value, Args);
next_state(State, Value, {call, _, cons, Args}) ->
    in_next(State, Value, Args);
next_state(State, Value, {call, _, head, Args}) ->
    get_next(State, Value, Args);
next_state(State, Value, {call, _, tail, Args}) ->
    drop_next(State, Value, Args);
next_state(State, Value, {call, _, snoc, Args}) ->
    in_r_next(State, Value, Args);
next_state(State, Value, {call, _, daeh, Args}) ->
    get_r_next(State, Value, Args);
next_state(State, Value, {call, _, last, Args}) ->
    get_r_next(State, Value, Args);
next_state(State, Value, {call, _, liat, Args}) ->
    drop_r_next(State, Value, Args);
next_state(State, Value, {call, _, init, Args}) ->
    drop_r_next(State, Value, Args);
next_state(State, Value, {call, _, lait, Args}) ->
    drop_r_next(State, Value, Args);
next_state(State, Value, {call, _, time, Args}) ->
    time_next(State, Value, Args);
next_state(State, Value, {call, _, timeout, Args}) ->
    timeout_next(State, Value, Args);
next_state(State, Value, {call, _, is_empty, Args}) ->
    is_empty_next(State, Value, Args);
next_state(State, Value, {call, _, len, Args}) ->
    len_next(State, Value, Args);
next_state(State, Value, {call, _, to_list, Args}) ->
    to_list_next(State, Value, Args);
next_state(State, Value, {call, _, member, Args}) ->
    member_next(State, Value, Args).

postcondition(State, {call, _, new, Args}, Result) ->
    new_post(State, Args, Result);
postcondition(State, {call, _, from_list, Args}, Result) ->
    from_list_post(State, Args, Result);
postcondition(State, {call, _, in, Args}, Result) ->
    in_post(State, Args, Result);
postcondition(State, {call, _, in_r, Args}, Result) ->
    in_r_post(State, Args, Result);
postcondition(State, {call, _, out, Args}, Result) ->
    out_post(State, Args, Result);
postcondition(State, {call, _, out_r, Args}, Result) ->
    out_r_post(State, Args, Result);
postcondition(State, {call, _, join, Args}, Result) ->
    join_post(State, Args, Result);
postcondition(State, {call, _, reverse, Args}, Result) ->
    reverse_post(State, Args, Result);
postcondition(State, {call, _, split, Args}, Result) ->
    split_post(State, Args, Result);
postcondition(State, {call, _, filter, Args}, Result) ->
    filter_post(State, Args, Result);
postcondition(State, {call, _, get, Args}, Result) ->
    get_post(State, Args, Result);
postcondition(State, {call, _, get_empty, Args}, Result) ->
    get_empty_post(State, Args, Result);
postcondition(State, {call, _, get_r, Args}, Result) ->
    get_r_post(State, Args, Result);
postcondition(State, {call, _, get_r_empty, Args}, Result) ->
    get_r_empty_post(State, Args, Result);
postcondition(State, {call, _, peek, Args}, Result) ->
    peek_post(State, Args, Result);
postcondition(State, {call, _, peek_r, Args}, Result) ->
    peek_r_post(State, Args, Result);
postcondition(State, {call, _, drop, Args}, Result) ->
    drop_post(State, Args, Result);
postcondition(State, {call, _, drop_empty, Args}, Result) ->
    drop_empty_post(State, Args, Result);
postcondition(State, {call, _, drop_r, Args}, Result) ->
    drop_r_post(State, Args, Result);
postcondition(State, {call, _, drop_r_empty, Args}, Result) ->
    drop_r_empty_post(State, Args, Result);
postcondition(State, {call, _, cons, Args}, Result) ->
    in_post(State, Args, Result);
postcondition(State, {call, _, head, Args}, Result) ->
    get_post(State, Args, Result);
postcondition(State, {call, _, tail, Args}, Result) ->
    drop_post(State, Args, Result);
postcondition(State, {call, _, snoc, Args}, Result) ->
    in_r_post(State, Args, Result);
postcondition(State, {call, _, daeh, Args}, Result) ->
    get_r_post(State, Args, Result);
postcondition(State, {call, _, last, Args}, Result) ->
    get_r_post(State, Args, Result);
postcondition(State, {call, _, liat, Args}, Result) ->
    drop_r_post(State, Args, Result);
postcondition(State, {call, _, init, Args}, Result) ->
    drop_r_post(State, Args, Result);
postcondition(State, {call, _, lait, Args}, Result) ->
    drop_r_post(State, Args, Result);
postcondition(State, {call, _, time, Args}, Result) ->
    time_post(State, Args, Result);
postcondition(State, {call, _, timeout, Args}, Result) ->
    timeout_post(State, Args, Result);
postcondition(State, {call, _, is_empty, Args}, Result) ->
    is_empty_post(State, Args, Result);
postcondition(State, {call, _, len, Args}, Result) ->
    len_post(State, Args, Result);
postcondition(State, {call, _, to_list, Args}, Result) ->
    to_list_post(State, Args, Result);
postcondition(State, {call, _, member, Args}, Result) ->
    member_post(State, Args, Result).

new_time() ->
    choose(0, 10).

item() ->
    oneof([a, b, c]).

incr_time(Time) ->
    ?LET(Incr, choose(0, 3), Time + Incr).

new_args(#state{mod=Mod}) ->
    Args = [Mod:module(), Mod:args()],
    oneof([[new_time() | Args], Args]).

new_pre(#state{squeue=S}, _Args) ->
    S =:= undefined.

new_next(State, Value, [Time, SMod, Args]) ->
    VS = {call, erlang, element, [2, Value]},
    new_next(State#state{time=Time}, VS, [SMod, Args]);
new_next(#state{mod=Mod} = State, VS, [_SMod, Args]) ->
    State#state{squeue=VS, list=[], mod_state=Mod:init(Args)}.

new_post(_State, [_Time, _Mod, _Args], {Drops, S}) ->
    Drops =:= [] andalso squeue:is_queue(S);
new_post(_state, [_Mod, _Args], S) ->
    squeue:is_queue(S).

from_list_args(#state{mod=Mod}) ->
    Args = [list(item()), Mod:module(), Mod:args()],
    oneof([[new_time() | Args], Args]).

from_list_pre(#state{squeue=S}, _Args) ->
    S =:= undefined.

from_list_next(State, Value, [Time, L, SMod, Args]) ->
    VS = {call, erlang, element, [2, Value]},
    from_list_next(State#state{time=Time}, VS, [L, SMod, Args]);
from_list_next(#state{mod=Mod} = State, VS, [L, _SMod, Args]) ->
    L2 = [{0, Item} || Item <- L],
    State#state{squeue=VS, list=L2, mod_state=Mod:init(Args)}.

from_list_post(_State, [_Time, _List, _Mod, _Args], {Drops, S}) ->
    Drops =:= [] andalso squeue:is_queue(S);
from_list_post(_state, [_List, _Mod, _Args], S) ->
    squeue:is_queue(S).

in_args(#state{time=Time, squeue=S}) ->
    oneof([[incr_time(Time), item(), S],
           [item(), S]]).

in_pre(#state{time=Time}, [NTime, _Item, _S]) ->
    NTime >= Time;
in_pre(_State, _Args) ->
    true.

in_next(#state{list=L} = State, VS, [Item, _S]) ->
    State#state{list=L ++ [{0, Item}], squeue=VS};
in_next(State, Value, [Time, Item, _S]) ->
    VS = {call, erlang, element, [2, Value]},
    #state{list=L} = NState = advance_time_state(State, Time),
    NState#state{list=L ++ [{0, Item}], squeue=VS}.

in_post(_State, [_Item, _S], NS) ->
    squeue:is_queue(NS);
in_post(State, [Time, _Item, _S], {Drops, NS}) ->
    case advance_time_drops(State, Time) =:= Drops andalso squeue:is_queue(NS) of
        true ->
            true;
        false ->
            false
    end.

in_r_args(State) ->
    in_args(State).

in_r_pre(#state{list=L}, [_Item, _S]) ->
    case L of
        [] ->
            true;
        [{0, _} | _] ->
            true;
        _ ->
            false
    end;
in_r_pre(#state{time=Time, list=L}, [NTime, _Item, _S]) ->
    case L of
        [] ->
            NTime >= Time;
        [{0, _} | _] ->
            %% Head must have been added at the same Time otherwise order of
            %% queue not maintained - i.e. head has sojourn time of 0 at
            %% insertion.
            NTime =:= Time;
        _ ->
            false
    end.

in_r_next(#state{list=L} = State, VS, [Item, _S]) ->
    State#state{list=[{0, Item} | L], squeue=VS};
in_r_next(State, Value, [Time, Item, _S]) ->
    VS = {call, erlang, element, [2, Value]},
    #state{list=L} = NState = advance_time_state(State, Time),
    NState#state{list=[{0, Item} | L], squeue=VS}.

in_r_post(_State, [_Item, _S], NS) ->
    squeue:is_queue(NS);
in_r_post(State, [Time, _Item, _S], {Drops, NS}) ->
    case advance_time_drops(State, Time) =:= Drops andalso squeue:is_queue(NS) of
        true ->
            true;
        false ->
            false
    end.

out_args(#state{time=Time, squeue=S}) ->
    oneof([[incr_time(Time), S],
           [S]]).

out_pre(#state{time=Time}, [NTime, _S]) ->
    NTime >= Time;
out_pre(_State, _Args) ->
    true.

out_next(State, Value, [_S]) ->
    VS = {call, erlang, element, [3, Value]},
    case prepare_out_state(State) of
        #state{list=[]} = NState ->
            NState#state{squeue=VS};
        #state{list=[_Out | NL]} = NState ->
            NState#state{list=NL, squeue=VS}
    end;
out_next(State, Value, [Time, S]) ->
    NState = advance_time_state(State, Time),
    out_next(NState, Value, [S]).

out_post(State, [_S], {Result, Drops, NS}) ->
    case prepare_out(State) of
        {Drops, #state{list=[]}} ->
            Result =:= empty andalso squeue:is_queue(NS);
        {Drops, #state{list=[Result|_]}} ->
            squeue:is_queue(NS);
        _ ->
            false
    end;
out_post(State, [Time, S], {Result, Drops, NS}) ->
    {Drops2, NState} = advance_time(State, Time),
    case lists:prefix(Drops2, Drops) of
        true ->
            {_, Drops3} = lists:split(length(Drops2), Drops),
            out_post(NState, [S], {Result, Drops3, NS});
        false ->
            false
    end.

out_r_args(State) ->
    out_args(State).

out_r_pre(State, Args) ->
    out_pre(State, Args).

out_r_next(State, Value, [_S]) ->
    VS = {call, erlang, element, [3, Value]},
    case prepare_out_state(State) of
        #state{list=[]} = NState ->
            NState#state{squeue=VS};
        #state{list=L} = NState ->
            NState#state{list=droplast(L), squeue=VS}
    end;
out_r_next(State, Value, [Time, S]) ->
    NState = advance_time_state(State, Time),
    out_r_next(NState, Value, [S]).

out_r_post(State, [_S], {Result, Drops, NS}) ->
    case prepare_out(State) of
        {Drops, #state{list=[]}} ->
            Result =:= empty andalso squeue:is_queue(NS);
        {Drops, #state{list=L}} ->
            Result =:= lists:last(L) andalso squeue:is_queue(NS);
        _ ->
            false
    end;
out_r_post(State, [Time, S], {Result, Drops, NS}) ->
    {Drops2, NState} = advance_time(State, Time),
    case lists:prefix(Drops2, Drops) of
        true ->
            {_, Drops3} = lists:split(length(Drops2), Drops),
            out_r_post(NState, [S], {Result, Drops3, NS});
        false ->
            false
    end.

join(S1, {Time2, StartList, Module, Args}) ->
    S2 = squeue:from_start_list(Time2, StartList, Module, Args),
    squeue:join(S1, S2).

join(NTime, S1, {Time2, StartList, Module, Args}) ->
    S2 = squeue:from_start_list(Time2, StartList, Module, Args),
    squeue:join(NTime, S1, S2).

join_list(MinStart, MaxStart) ->
    ?LET(Unordered, list(join_item(MinStart, MaxStart)),
         lists:sort(fun start_sort/2, Unordered)).

start_sort({T1, _Item1}, {T2, _Item2}) ->
    T1 =< T2.

drop_sort({T1, _Item1}, {T2, _Item2}) ->
    T1 >= T2.

join_item(MinStart, MaxStart) ->
    {choose(MinStart, MaxStart), item()}.

join_queue(Time, MinStart, Mod) ->
    ?LET({JoinList, Args}, {join_list(MinStart, Time), Mod:args()},
         {Time, JoinList, Mod:module(), Args}).

join_queue_incr(Time, MinStart, Mod) ->
    ?LET({NTime, JoinList, Args},
         {incr_time(Time), join_list(MinStart, Time), Mod:args()},
         {NTime, JoinList, Mod:module(), Args}).

join_args(#state{mod=Mod, list=L, time=Time, squeue=S}) ->
    %% Add default item that has been in queue whole time so that empty queue
    %% has MinStart of 0.
    {SojournTime, _} = lists:last([{Time, a} | L]),
    MinStart = Time-SojournTime,
    oneof([[S, join_queue(Time, MinStart, Mod)],
           [incr_time(Time), S, join_queue_incr(Time, MinStart, Mod)]]).

join_pre(#state{time=Time, list=L1}, [_S1, {Time, StartList, _, _}]) ->
     L2 = [{Time - Start, Item} || {Start, Item} <- StartList],
     do_join_pre(L1, L2);
join_pre(#state{mod=Mod, time=Time1} = State1,
         [NTime, _S1, {Time2, StartList, _, Args}])
  when NTime >= Time1 andalso NTime >= Time2 ->
    %% Create fake state for second squeue to do any dropping.
    ModState2 = Mod:init(Args),
    L2 = [{Time2 - Start, Item} || {Start, Item} <- StartList],
    State2 = #state{mod=Mod, mod_state=ModState2, time=Time2, list=L2},
    #state{list=NL1} = advance_time_state(State1, NTime),
    #state{list=NL2} = advance_time_state(State2, NTime),
    do_join_pre(NL1, NL2);
%% NTime < Time1 or NTime < Time2 or Time1 =/= Time2 when no NTime.
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
    State#state{list=L1++L2, squeue=VS};
join_next(#state{mod=Mod} = State1, Value,
          [NTime, _S1, {Time2, StartList, _, Args}]) ->
    VS = {call, erlang, element, [2, Value]},
    ModState2 = Mod:init(Args),
    L2 = [{Time2 - Start, Item} || {Start, Item} <- StartList],
    State2 = #state{mod=Mod, mod_state=ModState2, time=Time2, list=L2},
    #state{list=NL1} = NState1 = advance_time_state(State1, NTime),
    #state{list=NL2} = advance_time_state(State2, NTime),
    NState1#state{list=NL1 ++ NL2, squeue=VS}.

join_post(_State, [_S1, _S2], VS) ->
    squeue:is_queue(VS);
join_post(#state{mod=Mod} = State1, [NTime, _S1, {Time2, StartList, _, Args}],
          {Drops, NS}) ->
    L2 = [{Time2 - Start, Item} || {Start, Item} <- StartList],
    ModState2 = Mod:init(Args),
    State2 = #state{mod=Mod, mod_state=ModState2, time=Time2, list=L2},
    Drops1 = advance_time_drops(State1, NTime),
    Drops2 = advance_time_drops(State2, NTime),
    Drops3 = lists:sort(fun drop_sort/2, Drops1 ++ Drops2),
    Drops3 =:= Drops andalso squeue:is_queue(NS).

reverse_args(#state{time=Time, squeue=S}) ->
    oneof([[incr_time(Time), S], [S]]).

reverse_pre(#state{list=[], time=Time}, [NTime, _S]) ->
    NTime >= Time;
reverse_pre(#state{time=Time} = State, [NTime, S]) ->
    NTime >= Time andalso reverse_pre(State, [S]);
reverse_pre(#state{list=[]}, [_S]) ->
    true;
reverse_pre(#state{list=[{SojournTime, _} | L]}, [_S]) ->
    SameSojourn = fun({SojournTime2, _}) -> SojournTime2 =:= SojournTime end,
    L =:= lists:filter(SameSojourn, L).

reverse_next(#state{list=L} = State, VS, [_S]) ->
    State#state{list=lists:reverse(L), squeue=VS};
reverse_next(State, Value, [Time, _S]) ->
    VS = {call, erlang, element, [2, Value]},
    #state{list=L} = NState = advance_time_state(State, Time),
    NState#state{list=lists:reverse(L), squeue=VS}.

reverse_post(_State, [_S], NS) ->
    squeue:is_queue(NS);
reverse_post(State, [Time, _S], {Drops, NS}) ->
    advance_time_drops(State, Time) =:= Drops andalso squeue:is_queue(NS).

split_args(#state{time=Time, list=L, squeue=S}) ->
    oneof([[incr_time(Time), choose(0, length(L)), S],
           [choose(0, length(L)), S]]).

split_pre(State, [N, _S]) ->
    #state{list=L} = prepare_out_state(State),
    N =< length(L);
split_pre(#state{time=Time} = State, [NTime, N, S]) when NTime >= Time ->
    NState= advance_time_state(State, NTime),
    split_pre(NState, [N, S]);
split_pre(_State, _Args) ->
    false.

split_next(State, Value, [N, _S]) ->
    Elem = (N rem 2) + 1,
    VS = {call, erlang, element, [Elem + 1, Value]},
    #state{list=L} = NState = prepare_out_state(State),
    NL = element(Elem, lists:split(N, L)),
    NState#state{list=NL, squeue=VS};
split_next(State, Value, [Time, N, S]) ->
    NState = advance_time_state(State, Time),
    split_next(NState, Value, [N, S]).

split_post(State, [_N, _S], {Drops, S2, S3}) ->
    prepare_out_drops(State) =:= Drops andalso
    squeue:is_queue(S2) andalso squeue:is_queue(S3);
split_post(State, [Time, N, S], {Drops, S2, S3}) ->
    {Drops2, NState} = advance_time(State, Time),
    case lists:prefix(Drops2, Drops) of
        true ->
            {_, Drops3} = lists:split(length(Drops2), Drops),
            split_post(NState, [N, S], {Drops3, S2, S3});
        false ->
            false
    end.

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
    oneof([[incr_time(Time) | Args], Args]).

filter_pre(#state{time=Time}, [NTime, _Action, _Item, _S]) ->
    NTime >= Time;
filter_pre(_State, [_Action, _Item, _S]) ->
    true.

filter_next(State, Value, [duplicate, Item, _S]) ->
    VS = {call, erlang, element, [2, Value]},
    #state{list=L} = NState = prepare_out_state(State),
    Duplicate = fun({_, Item2} = Elem) when Item2 =:= Item ->
                        [Elem, Elem];
                   (Other) ->
                        [Other]
                end,
    NL = lists:flatmap(Duplicate, L),
    NState#state{list=NL, squeue=VS};
filter_next(State, Value, [filter, Item, _S]) ->
    VS = {call, erlang, element, [2, Value]},
    #state{list=L} = NState = prepare_out_state(State),
    Filter = fun({_, Item2} ) -> Item2 =:= Item end,
    NL = lists:filter(Filter, L),
    NState#state{list=NL, squeue=VS};
filter_next(State, Value, [Time, Action, Item, S]) ->
    NState = advance_time_state(State, Time),
    filter_next(NState, Value, [Action, Item, S]).

filter_post(State, [_Action, _Item, _S], {Drops, NS}) ->
    prepare_out_drops(State) =:= Drops andalso squeue:is_queue(NS);
filter_post(State, [Time, Action, Item, S], {Drops, NS}) ->
    {Drops2, NState} = advance_time(State, Time),
    case lists:prefix(Drops2, Drops) of
        true ->
            {_, Drops3} = lists:split(length(Drops2), Drops),
            filter_post(NState, [Action, Item, S], {Drops3, NS});
        false ->
            false
    end.

get_args(#state{time=Time, squeue=S}) ->
    oneof([[incr_time(Time), S], [S]]).

get_pre(State, Args) ->
    drop_pre(State, Args).

get_next(State, _Value, _Args) ->
    State.

get_post(State, [_S], Result) ->
    case prepare_out_state(State) of
        #state{list=[{_, Result}|_]} ->
            true;
        _ ->
            false
    end;
get_post(State, [Time, S], Result) ->
    NState = advance_time_state(State, Time),
    get_post(NState, [S], Result).

get_empty(S) ->
    catch squeue:get(S).

get_empty(Time, S) ->
    catch squeue:get(Time, S).

get_empty_args(State) ->
    get_args(State).

get_empty_pre(#state{time=Time}, [NTime, _S]) when NTime < Time ->
    false;
get_empty_pre(State, Args) ->
    not get_pre(State, Args).

get_empty_next(State, _Value, _Args) ->
    State.

get_empty_post(_State, _Args, {'EXIT', {empty, _}}) ->
    true;
get_empty_post(_State, _Args, _Result) ->
    false.

get_r_args(State) ->
    get_args(State).

get_r_pre(State, Args) ->
    get_pre(State, Args).

get_r_next(State, _Value, _Args) ->
    State.

get_r_post(State, [_S], Result) ->
    #state{list=L} = prepare_out_state(State),
    {_, Item} = lists:last(L),
    Item =:= Result;
get_r_post(State, [Time, S], Result) ->
    NState = advance_time_state(State, Time),
    get_r_post(NState, [S], Result).

get_r_empty(S) ->
    catch squeue:get_r(S).

get_r_empty(Time, S) ->
    catch squeue:get_r(Time, S).

get_r_empty_args(State) ->
    get_empty_args(State).

get_r_empty_pre(State, Args) ->
    get_empty_pre(State, Args).

get_r_empty_next(State, _Value, _Args) ->
    State.

get_r_empty_post(State, Args, Result) ->
    get_empty_post(State, Args, Result).

peek_args(#state{time=Time, squeue=S}) ->
    oneof([[incr_time(Time), S], [S]]).

peek_pre(_State, [_S]) ->
    true;
peek_pre(#state{time=Time}, [NTime, _S]) ->
    NTime >= Time.

peek_next(State, _Value, _Args) ->
    State.

peek_post(State, [_S], Result) ->
    case prepare_out_state(State) of
        #state{list=[]} ->
            Result =:= empty;
        #state{list=[Result | _]} ->
            true;
        _ ->
            false
    end;
peek_post(State, [Time, S], Result) ->
    NState = advance_time_state(State, Time),
    peek_post(NState, [S], Result).

peek_r_args(State) ->
    peek_args(State).

peek_r_pre(State, Args) ->
    peek_pre(State, Args).

peek_r_next(State, _Value, _Args) ->
    State.

peek_r_post(State, [_S], Result) ->
    case prepare_out_state(State) of
        #state{list=[]} ->
            Result =:= empty;
        #state{list=L} ->
            lists:last(L) =:= Result
    end;
peek_r_post(State, [Time, S], Result) ->
    NState = advance_time_state(State, Time),
    peek_r_post(NState, [S], Result).

drop_args(#state{time=Time, squeue=S}) ->
    oneof([[incr_time(Time), S], [S]]).

drop_pre(State, [_S]) ->
    case prepare_out_state(State) of
        #state{list=[_|_]} ->
            true;
        _ ->
            false
    end;
drop_pre(#state{time=Time} = State, [NTime, S]) when NTime >= Time ->
    NState = advance_time_state(State, NTime),
    drop_pre(NState, [S]);
drop_pre(_State, _Args) ->
    false.

drop_next(State, Value, [_S]) ->
    VS = {call, erlang, element, [2, Value]},
    #state{list=[_ | L]} = NState = prepare_out_state(State),
    NState#state{list=L, squeue=VS};
drop_next(State, Value, [Time, S]) ->
    NState = advance_time_state(State, Time),
    drop_next(NState, Value, [S]).

drop_post(State, [_S], {Drops, NS}) ->
     prepare_out_drops(State) =:= Drops andalso squeue:is_queue(NS);
drop_post(State, [Time, S], {Drops, NS}) ->
    {Drops2, NState} = advance_time(State, Time),
    case lists:prefix(Drops2, Drops) of
        true ->
            {_, Drops3} = lists:split(length(Drops2), Drops),
            drop_post(NState, [S], {Drops3, NS});
        false ->
            false
    end.

drop_empty(S) ->
    catch squeue:drop(S).

drop_empty(Time, S) ->
    catch squeue:drop(Time, S).

drop_empty_args(State) ->
    drop_args(State).

drop_empty_pre(#state{time=Time}, [NTime, _S]) when NTime < Time ->
    false;
drop_empty_pre(State, Args) ->
    not drop_pre(State, Args).

drop_empty_next(State, _Value, _Args) ->
    State.

drop_empty_post(_State, _Args, {'EXIT', {empty, _}}) ->
    true;
drop_empty_post(_State, _Args, _Result) ->
    false.

drop_r_args(State) ->
    drop_args(State).

drop_r_pre(State, Args) ->
    drop_pre(State, Args).

drop_r_next(State, Value, [_S]) ->
    VS = {call, erlang, element, [2, Value]},
    #state{list=L} = NState = prepare_out_state(State),
    NState#state{list=droplast(L), squeue=VS};
drop_r_next(State, Value, [Time, S]) ->
    NState = advance_time_state(State, Time),
    drop_r_next(NState, Value, [S]).

drop_r_post(State, Args, Result) ->
    drop_post(State, Args, Result).

drop_r_empty(S) ->
    catch squeue:drop_r(S).

drop_r_empty(Time, S) ->
    catch squeue:drop_r(Time, S).

drop_r_empty_args(State) ->
    drop_empty_args(State).

drop_r_empty_pre(State, Args) ->
    drop_empty_pre(State, Args).

drop_r_empty_next(State, _Value, _Args) ->
    State.

drop_r_empty_post(State, Args, Result) ->
    drop_empty_post(State, Args, Result).

time_args(#state{squeue=S}) ->
    [S].

time_pre(_State, _Args) ->
    true.

time_next(State, _Value, _Args) ->
    State.

time_post(#state{time=Time}, _Args, Time2) ->
    Time =:= Time2.

timeout_args(#state{time=Time, squeue=S}) ->
    [incr_time(Time), S].

timeout_pre(#state{time=Time}, [NTime, _S]) ->
      NTime >= Time.

timeout_next(State, Value, [Time, _S]) ->
    VS = {call, erlang, element, [2, Value]},
    NState = advance_time_state(State, Time),
    NState#state{squeue=VS}.

timeout_post(State, [Time, _S], {Drops, NS}) ->
    advance_time_drops(State, Time) =:= Drops andalso squeue:is_queue(NS).

is_empty_args(#state{time=Time, squeue=S}) ->
    oneof([[incr_time(Time), S],
           [S]]).

is_empty_pre(#state{time=Time}, [NTime, _S]) ->
    NTime >= Time;
is_empty_pre(_State, _Args) ->
    true.

is_empty_next(State, _Value, _Args) ->
    State.

is_empty_post(#state{list=L}, [_S], Bool) ->
    case L of
        [] ->
            Bool =:= true;
        _ ->
            Bool =:= false
    end;
is_empty_post(State, [Time, _S], Bool) ->
    case advance_time_state(State, Time) of
        #state{list=[]} ->
            Bool =:= true;
        _ ->
            Bool =:= false
    end.

len_args(#state{time=Time, squeue=S}) ->
    oneof([[incr_time(Time), S],
           [S]]).

len_pre(#state{time=Time}, [NTime, _S]) ->
    NTime >= Time;
len_pre(_State, _Args) ->
    true.

len_next(State, _Value, _Args) ->
    State.

len_post(#state{list=L}, [_S], Len) ->
    length(L) =:= Len;
len_post(State, [Time, _S], Len) ->
    #state{list=L} = advance_time_state(State, Time),
    length(L) =:= Len.

to_list_args(#state{time=Time, squeue=S}) ->
    oneof([[incr_time(Time), S],
           [S]]).

to_list_pre(#state{time=Time}, [NTime, _S]) ->
    NTime >= Time;
to_list_pre(_State, _Args) ->
    true.

to_list_next(State, _Value, _Args) ->
    State.

to_list_post(#state{list=L}, [_S], List) ->
    {_, Items} = lists:unzip(L),
    Items =:= List;
to_list_post(State, [Time, _S], List) ->
    #state{list=L} = advance_time_state(State, Time),
    {_, Items} = lists:unzip(L),
    Items =:= List.

member_args(#state{time=Time, squeue=S}) ->
    oneof([[incr_time(Time), item(), S],
           [item(), S]]).

member_pre(#state{time=Time}, [NTime, _Item, _S]) ->
    NTime >= Time;
member_pre(_State, _Args) ->
    true.

member_next(State, _Value, _Args) ->
    State.

member_post(#state{list=L}, [Item, _S], Bool) ->
    {_, Items} = lists:unzip(L),
    Bool =:= lists:member(Item, Items);
member_post(State, [Time, Item, S], Bool) ->
    NState = advance_time_state(State, Time),
    member_post(NState, [Item, S], Bool).

advance_time_state(State, Time) ->
    {_, NState} = advance_time(State, Time),
    NState.

advance_time_drops(State, Time) ->
    {Drops, _} = advance_time(State, Time),
    Drops.

advance_time(#state{time=Time} = State, Time) ->
    {[], State};
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
