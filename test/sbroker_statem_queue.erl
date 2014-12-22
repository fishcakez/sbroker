-module(sbroker_statem_queue).

-behaviour(squeue).

-export([init/1]).
-export([handle_timeout/3]).
-export([handle_out/3]).
-export([handle_join/3]).


%% This squeue backend takes a list of non_neg_integer() and drops the integer
%% at head of the list (or the whole queue if the queue length is lower). The
%% tail is kept and used for the next call. Once the list is emptied the
%% original list is used in its place, if this list is empty no drops occcur.
%%
%% Time is ignored completely to allow testing independent of time in sbroker.
%% Timing in `squeue` is tested in squeue_statem where the test controls the
%% time.

init(Drops) ->
    {Drops, Drops}.

handle_timeout(_, Q, State) ->
    handle(Q, State).

handle_out(_, Q, State) ->
    handle(Q, State).

handle_join(_, Q, State) ->
    {[], Q, State}.

%% If queue is empty don't change state.
handle(Q, State) ->
    case queue:is_empty(Q) of
        true ->
            {[], Q, State};
        false ->
            do_handle(Q, State)
    end.

do_handle(Q, {[], []} = State) ->
    {[], Q, State};
do_handle(Q, {[], Drops}) ->
    do_handle(Q, {Drops, Drops});
do_handle(Q, {[N | Rest], Drops}) ->
    Split = min(N, queue:len(Q)),
    {DropQ, NQ} = queue:split(Split, Q),
    {queue:to_list(DropQ), NQ, {Rest, Drops}}.
