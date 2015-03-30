-module(svalve_codel_r_statem).

-include_lib("proper/include/proper.hrl").

-export([quickcheck/0]).
-export([quickcheck/1]).
-export([check/1]).
-export([check/2]).

-export([module/0]).
-export([args/0]).

-export([init/1]).
-export([handle_sojourn/6]).
-export([handle_sojourn_r/6]).
-export([handle_sojourn_closed/6]).
-export([handle_dropped/5]).
-export([handle_dropped_r/5]).
-export([handle_dropped_closed/5]).

-export([initial_state/0]).
-export([command/1]).
-export([precondition/2]).
-export([next_state/3]).
-export([postcondition/3]).

-record(state, {target, interval, first_below_time=undefined,
                dequeue_next=undefined, count=0, dequeuing=false, status=open,
                now=undefined}).

quickcheck() ->
    quickcheck([]).

quickcheck(Opts) ->
    proper:quickcheck(prop_svalve(), Opts).

check(CounterExample) ->
    check(CounterExample, []).

check(CounterExample, Opts) ->
    proper:check(prop_svalve(), CounterExample, Opts).

prop_svalve() ->
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



module() ->
    svalve_codel_r.

args() ->
    {choose(0, 3), choose(1, 3)}.


init({Target, Interval}) ->
    #state{target=Target, interval=Interval}.

handle_sojourn(Time, SojournTime, Q, Manager, ManState, State) ->
    NState = State#state{now=Time, status=open},
    {Status, NState2} = do_compare(SojournTime, NState),
    case NState2#state.dequeuing of
        true ->
            handle_dequeuing(Status, handle_out, Q, Manager, ManState, NState2);
        false ->
            handle_not_dequeuing(Status, handle_out, Q, Manager, ManState,
                                 NState2)
    end.

handle_sojourn_r(Time, SojournTime, Q, Manager, ManState, State) ->
    NState = State#state{now=Time, status=open},
    {Status, NState2} = do_compare(SojournTime, NState),
    case NState2#state.dequeuing of
        true ->
            handle_dequeuing(Status, handle_out_r, Q, Manager, ManState,
                             NState2);
        false ->
            handle_not_dequeuing(Status, handle_out_r, Q, Manager, ManState,
                                 NState2)
    end.

handle_sojourn_closed(Time, SojournTime, Q, Manager, ManState, State) ->
    NState = State#state{now=Time, status=closed},
    {Status, NState2} = do_compare(SojournTime, NState),
    case NState2#state.dequeuing of
        true ->
            handle_dequeuing(Status, handle_timeout, Q, Manager, ManState,
                             NState2);
        false ->
            handle_not_dequeuing(Status, handle_timeout, Q, Manager, ManState,
                                 NState2)
    end.

handle_dropped(Time, Q, Manager, ManState, #state{count=Count} = State) ->
    NState = State#state{first_below_time=undefined, dequeuing=false,
                         count=max(0, Count-1), status=open, now=Time},
    handle_not_dequeuing(closed, undefined, Q, Manager, ManState, NState).

handle_dropped_r(Time, Q, Manager, ManState, State) ->
    handle_dropped(Time, Q, Manager, ManState, State).

handle_dropped_closed(Time, Q, Manager, ManState,
                      #state{count=Count} = State) ->
    NState = State#state{first_below_time=undefined, dequeuing=false,
                         count=max(0, Count-1), status=closed, now=Time},
    handle_not_dequeuing(closed, undefined, Q, Manager, ManState, NState).

handle_dequeuing(closed, _, Q, Manager, ManState, #state{now=Now} = State) ->
    {DropCount, NManState} = Manager:handle_timeout(Now, Q, ManState),
    {closed, DropCount, NManState, State#state{dequeuing=false}};
handle_dequeuing(open, Fun, Q, Manager, ManState,
                 #state{dequeue_next=DequeueNext, status=Status, count=Count,
                        now=Now} = State)
  when Now >= DequeueNext ->
    case Manager:Fun(Now, Q, ManState) of
        {DropCount, NManState} when Status =:= closed ->
            {closed, DropCount, NManState, State};
        {DropCount, NManState} when length(Q) =:= DropCount ->
            {open, DropCount, NManState, State};
        {DropCount, NManState} ->
            NState = control_law(DequeueNext, State#state{count=Count+1}),
            {open, DropCount, NManState, NState}
    end;
handle_dequeuing(open, _, Q, Manager, ManState, #state{now=Now} = State) ->
    {DropCount, NManState} = Manager:handle_timeout(Now, Q, ManState),
    {closed, DropCount, NManState, State}.

handle_not_dequeuing(open, Fun, Q, Manager, ManState,
                     #state{interval=Interval, dequeue_next=DequeueNext,
                            status=Status, count=Count, now=Now} = State) ->
    case Manager:Fun(Now, Q, ManState) of
        {DropCount, NManState} when Status =:= closed ->
            {closed, DropCount, NManState, State};
        {DropCount, NManState} when length(Q) =:= DropCount ->
            {open, DropCount, NManState, State};
        {DropCount, NManState} ->
            NState = State#state{dequeuing=true},
            NCount = if
                         Count > 2 andalso Now - DequeueNext < Interval ->
                             Count - 2;
                         true ->
                             1
                     end,
            NState2 = control_law(Now, NState#state{count=NCount}),
            {open, DropCount, NManState, NState2}
    end;
handle_not_dequeuing(closed, _Fun, Q, Manager, ManState,
                     #state{now=Now} = State) ->
    {DropCount, NManState} = Manager:handle_timeout(Now, Q, ManState),
    {closed, DropCount, NManState, State}.


do_compare(SojournTime, #state{target=Target} = State)
  when SojournTime > Target ->
    {closed, State#state{first_below_time=undefined}};
do_compare(_,
           #state{interval=Interval, first_below_time=undefined,
                  now=Now} = State) ->
    FirstBelow = Now + Interval,
    {closed, State#state{first_below_time=FirstBelow}};
do_compare(_, #state{first_below_time=FirstBelow,
                     now=Now} = State)
  when Now >= FirstBelow ->
    {open, State};
do_compare(_, State) ->
    {closed, State}.

control_law(Start, #state{interval=Interval, count=Count} = State) ->
    DequeueNext = Start + erlang:trunc(Interval / math:sqrt(Count)),
    State#state{dequeue_next=DequeueNext}.

initial_state() ->
    squeue_statem:initial_state(svalve, ?MODULE, [squeue_timeout_statem,
                                                  squeue_naive_statem,
                                                  squeue_codel_statem,
                                                  squeue_codel_timeout_statem]).

command(State) ->
    squeue_statem:command(State).

precondition(State, Call) ->
    squeue_statem:precondition(State, Call).

next_state(State, Value, Call) ->
    squeue_statem:next_state(State, Value, Call).

postcondition(State, Call, Result) ->
    squeue_statem:postcondition(State, Call, Result).
