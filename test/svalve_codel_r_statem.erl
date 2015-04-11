-module(svalve_codel_r_statem).

-include_lib("proper/include/proper.hrl").

-export([quickcheck/0]).
-export([quickcheck/1]).
-export([check/1]).
-export([check/2]).

-export([module/0]).
-export([args/0]).

-export([init/1]).
-export([handle_sojourn/4]).
-export([handle_dropped/3]).

-export([initial_state/0]).
-export([command/1]).
-export([precondition/2]).
-export([next_state/3]).
-export([postcondition/3]).

-record(state, {target, interval, first_below_time=undefined,
                dequeue_next=undefined, count=0, dequeuing=false,
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

handle_sojourn(Time, SojournTime, Q, State) ->
    {Status, NState} = do_compare(SojournTime, State#state{now=Time}),
    case NState#state.dequeuing of
        true ->
            handle_dequeuing(Status, Q, NState);
        false ->
            handle_not_dequeuing(Status, Q, NState)
    end.

handle_dropped(Time, Q, #state{count=Count} = State) ->
    NState = State#state{first_below_time=undefined, dequeuing=false,
                         count=max(0, Count-1), now=Time},
    handle_not_dequeuing(closed, Q, NState).

handle_dequeuing(closed, _, State) ->
    {closed, State#state{dequeuing=false}};
handle_dequeuing(open, Q, #state{dequeue_next=DequeueNext, count=Count,
                                 now=Now} = State)
  when Now >= DequeueNext ->
    case Q of
        [_ | _] ->
            NState = control_law(DequeueNext, State#state{count=Count+1}),
            {open, NState};
        [] ->
            {open, State}
    end;
handle_dequeuing(open, _, State) ->
    {closed, State}.

handle_not_dequeuing(open, [_ | _],
                     #state{interval=Interval, dequeue_next=DequeueNext,
                            count=Count, now=Now} = State) ->
    NState = State#state{dequeuing=true},
    NCount = if
                 Count > 2 andalso Now - DequeueNext < Interval ->
                     Count - 2;
                 true ->
                     1
             end,
    NState2 = control_law(Now, NState#state{count=NCount}),
    {open, NState2};
handle_not_dequeuing(Status, _, State) ->
    {Status, State}.

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
    svalve_statem:initial_state(?MODULE).

command(State) ->
    svalve_statem:command(State).

precondition(State, Call) ->
    svalve_statem:precondition(State, Call).

next_state(State, Value, Call) ->
    svalve_statem:next_state(State, Value, Call).

postcondition(State, Call, Result) ->
    svalve_statem:postcondition(State, Call, Result).
