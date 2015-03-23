-module(svalve_naive_statem).

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
    svalve_naive.

args() ->
    undefined.

init(undefined) ->
    undefined.

handle_sojourn(Time, _, Q, Manager, ManState, undefined) ->
    {DropCount, NManState} = Manager:handle_out(Time, Q, ManState),
    {open, DropCount, NManState, undefined}.

handle_sojourn_r(Time, _, Q, Manager, ManState, undefined) ->
    {DropCount, NManState} = Manager:handle_out_r(Time, Q, ManState),
    {open, DropCount, NManState, undefined}.

handle_sojourn_closed(Time, _, Q, Manager, ManState, undefined) ->
    {DropCount, NManState} = Manager:handle_timeout(Time, Q, ManState),
    {closed, DropCount, NManState, undefined}.

handle_dropped(Time, Q, Manager, ManState, undefined) ->
    {DropCount, NManState} = Manager:handle_timeout(Time, Q, ManState),
    {closed, DropCount, NManState, undefined}.

handle_dropped_r(Time, Q, Manager, ManState, undefined) ->
    handle_dropped(Time, Q, Manager, ManState, undefined).

handle_dropped_closed(Time, Q, Manager, ManState, undefined) ->
    handle_dropped(Time, Q, Manager, ManState, undefined).

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
