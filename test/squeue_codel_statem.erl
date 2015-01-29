%% This modules impliments the CoDel algorithm by directly translating the CoDel
%% draft implementation at:
%% https://tools.ietf.org/html/draft-nichols-tsvwg-codel-02
%%
%% Copyright (C) 2011-2014 Kathleen Nichols <nichols@pollere.com>
%% Copyright (C) 2014 James Fish <james@fishcakez.com>
%%
%% Redistribution and use in source and binary forms, with or without
%% modification, are
%%
%% permitted provided that the following conditions are met:
%%
%% o  Redistributions of source code must retain the above copyright
%%    notice, this list of conditions, and the following disclaimer,
%%    without modification.
%%
%% o  Redistributions in binary form must reproduce the above copyright
%%    notice, this list of conditions and the following disclaimer in
%%    the documentation and/or other materials provided with the
%%    distribution.
%%
%% o  The names of the authors may not be used to endorse or promote
%%    products derived from this software without specific prior written
%%    permission.
%%
%% Alternatively, provided that this notice is retained in full, this
%% software may be distributed under the terms of the GNU General Public
%% License ("GPL") version 2, in which case the provisions of the GPL
%% apply INSTEAD OF those given above.
%%
%% THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
%% "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
%% LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
%% A PARTICULAR PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT
%% OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
%% SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
%% LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
%% DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
%% THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
%% (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
%% OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
-module(squeue_codel_statem).

-include_lib("proper/include/proper.hrl").

-export([quickcheck/0]).
-export([quickcheck/1]).
-export([check/1]).
-export([check/2]).

-export([module/0]).
-export([args/0]).
-export([init/1]).
-export([handle_timeout/3]).
-export([handle_out/3]).

-export([initial_state/0]).
-export([command/1]).
-export([precondition/2]).
-export([next_state/3]).
-export([postcondition/3]).

-record(state, {target, interval, first_above_time=0, drop_next=0, count=0,
                dropping=false, now=0}).

quickcheck() ->
    quickcheck([]).

quickcheck(Opts) ->
    proper:quickcheck(prop_squeue(), Opts).

check(CounterExample) ->
    check(CounterExample, []).

check(CounterExample, Opts) ->
    proper:check(prop_squeue(), CounterExample, Opts).

prop_squeue() ->
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
    squeue_codel.

args() ->
    {choose(1, 3), choose(0, 3)}.

init({Target, Interval}) ->
    #state{target=Target, interval=Interval}.

%% To ensure following the reference codel implementationas closely as possible
%% use the full dequeue approach and "undo" the following:
%% * Changing first_above_time to 0
%% * Changing from dropping true to false
%% This means that a slow queue is detected and items can be dropped but a
%% real dequeue is required to stop the first (or consecutive) slow intervals.
handle_timeout(Time, L, #state{first_above_time=0} = State) ->
    handle_out(Time, L, State);
handle_timeout(Time, L, #state{dropping=true, target=Target,
                               first_above_time=FirstAbove} = State) ->
    {N, NState} = handle_out(Time, L, State),
    case lists:split(N, L) of
        {[], _} ->
            %% No items dropped so state does not change.
            {0, State};
        {_, [Sojourn | _]} when Sojourn >= Target ->
            %% Next item is slow and was not dropped so still dropping.
            {N, NState};
        {Dropped, _} ->
            %% Next item is not below target or queue is empty, so dropping may
            %% have been set to false. Reverse any state changes that occured by
            %% observing this.
            case handle_out(Time, Dropped ++ [Target], State) of
                {N, NState2} ->
                    {N, NState2};
                _ ->
                    NState2 = NState#state{count=NState#state.count+1,
                                           dropping=true,
                                           first_above_time=FirstAbove},
                    NState3 = control_law(NState#state.drop_next, NState2),
                    {N, NState3}
            end
    end;
handle_timeout(Time, L, #state{dropping=false,
                               first_above_time=FirstAbove} = State) ->
    case handle_out(Time, L, State) of
        {0, NState} ->
            %% Head might be below target, or empty queue, maintain previous
            %% first_above_time.
            {0, NState#state{first_above_time=FirstAbove}};
        {1, NState} ->
            %% End of first interval resulted in drop. If new head is below
            %% target, or empty queue, first_above_time is reset, maintain
            %% previous. Dropping is always true after first drop.
            {1, NState#state{first_above_time=FirstAbove}}
    end.

handle_out(Time, L, State) ->
    {Item, NL, NState} = do_dequeue(L, State#state{now=Time}),
    case NState#state.dropping of
        true ->
            dequeue_dropping(Item, NL, NState);
        false ->
            dequeue_not_dropping(Item, NL, NState)
    end.

dequeue_dropping({nodrop, _} = Item, L, State) ->
    dequeue_dropping(Item, L, State#state{dropping=false}, 0);
dequeue_dropping(Item, L, State) ->
    dequeue_dropping(Item, L, State, 0).

dequeue_dropping(_Item, L, #state{dropping=Dropping, drop_next=DropNext,
                                    now=Now} = State, Drops)
  when Now >= DropNext andalso Dropping =:= true ->
    NDrops = Drops + 1,
    case do_dequeue(L, State) of
        {{nodrop, _} = NItem, NL, NState} ->
            dequeue_dropping(NItem, NL, NState#state{dropping=false}, NDrops);
        {{drop, _} = NItem, NL, #state{count=Count} = NState} ->
            NState2 = control_law(DropNext, NState#state{count=Count+1}),
            dequeue_dropping(NItem, NL, NState2, NDrops)
    end;
dequeue_dropping(_Item, _L, State, Drops) ->
    {Drops, State}.

dequeue_not_dropping({drop, _Item}, L,
                     #state{interval=Interval, drop_next=DropNext,
                            count=Count, now=Now} = State) ->
    Drops = 1,
    {_, _, NState} = do_dequeue(L, State),
    NState2 = NState#state{dropping=true},
    NCount = if
                 Count > 2 andalso Now - DropNext < Interval ->
                     Count - 2;
                 true ->
                     1
             end,
    {Drops, control_law(Now, NState2#state{count=NCount})};
dequeue_not_dropping({nodrop, _}, _L, State) ->
    {0, State}.

control_law(Start, #state{interval=Interval, count=Count} = State) ->
    DropNext = Start + erlang:trunc(Interval / math:sqrt(Count)),
    State#state{drop_next=DropNext}.

do_dequeue([], State) ->
    {{nodrop, empty}, [], State#state{first_above_time=0}};
do_dequeue([SojournTime | L], #state{target=Target} = State)
  when SojournTime < Target ->
    {{nodrop, SojournTime}, L, State#state{first_above_time=0}};
do_dequeue([SojournTime | L], #state{interval=Interval, first_above_time=0,
                                     now=Now} = State) ->
    FirstAbove = Now + Interval,
    {{nodrop, SojournTime}, L, State#state{first_above_time=FirstAbove}};
do_dequeue([SojournTime | L], #state{first_above_time=FirstAbove,
                                     now=Now} = State)
  when Now >= FirstAbove ->
    {{drop, SojournTime}, L, State};
do_dequeue([SojournTime | L], State) ->
    {{nodrop, SojournTime}, L, State}.

initial_state() ->
    squeue_statem:initial_state(squeue, ?MODULE).

command(State) ->
    squeue_statem:command(State).

precondition(State, Call) ->
    squeue_statem:precondition(State, Call).

next_state(State, Value, Call) ->
    squeue_statem:next_state(State, Value, Call).

postcondition(State, Call, Result) ->
    squeue_statem:postcondition(State, Call, Result).
