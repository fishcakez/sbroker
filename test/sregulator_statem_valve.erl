-module(sregulator_statem_valve).

-behaviour(svalve).

-export([init/2]).
-export([handle_sojourn/4]).
-export([handle_sojourn_r/4]).
-export([handle_sojourn_closed/4]).
-export([handle_dropped/3]).
-export([handle_dropped_r/3]).
-export([handle_dropped_closed/3]).

init(_, Sequence) ->
    {Sequence, Sequence}.

handle_sojourn(Time, _, S, State) ->
    handle(Time, S, State).

handle_sojourn_r(Time, _, S, State) ->
    handle_r(Time, S, State).

handle_sojourn_closed(Time, _, S, State) ->
    handle_closed(Time, S, State).

handle_dropped(Time, S, State) ->
    handle(Time, S, State).

handle_dropped_r(Time, S, State) ->
    handle_r(Time, S, State).

handle_dropped_closed(Time, S, State) ->
    handle_closed(Time, S, State).


handle(Time, S, {[], []} = State) ->
    {Drops, NS} = squeue:timeout(Time, S),
    {closed, Drops, NS, State};
handle(Time, S, {[], Sequence}) ->
    handle(Time, S, {Sequence, Sequence});
handle(Time, S, {[open | Rest], Sequence}) ->
    {Result, Drops, NS} = squeue:out(Time, S),
    {Result, Drops, NS, {Rest, Sequence}};
handle(Time, S, {[closed | Rest], Sequence}) ->
    {Drops, NS} = squeue:timeout(Time, S),
    {closed, Drops, NS, {Rest, Sequence}}.

handle_r(Time, S, {[], []} = State) ->
    {Drops, NS} = squeue:timeout(Time, S),
    {closed, Drops, NS, State};
handle_r(Time, S, {[], Sequence}) ->
    handle_r(Time, S, {Sequence, Sequence});
handle_r(Time, S, {[open | Rest], Sequence}) ->
    {Result, Drops, NS} = squeue:out_r(Time, S),
    {Result, Drops, NS, {Rest, Sequence}};
handle_r(Time, S, {[closed | Rest], Sequence}) ->
    {Drops, NS} = squeue:timeout(Time, S),
    {closed, Drops, NS, {Rest, Sequence}}.

handle_closed(Time, S, State) ->
    {Drops, NS} = squeue:timeout(Time, S),
    {closed, Drops, NS, State}.
