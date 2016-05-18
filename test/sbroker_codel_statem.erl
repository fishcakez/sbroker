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
-module(sbroker_codel_statem).

-include_lib("proper/include/proper.hrl").

-export([module/0]).
-export([args/0]).
-export([time_dependence/1]).
-export([init/1]).
-export([handle_timeout/3]).
-export([handle_out/3]).
-export([handle_out_r/3]).
-export([config_change/3]).

-record(state, {target, interval, max_packet, count=0, drop_next=undefined,
                first_above_time=undefined, dropping=false, now=undefined}).

module() ->
    sbroker_codel_queue.

args() ->
    ?SUCHTHAT({_, _, _, _, Min, Max},
              {oneof([out, out_r]),
               choose(0, 3),
               choose(1, 3),
               oneof([drop, drop_r]),
               choose(0, 3),
               oneof([choose(0, 5), infinity])},
              Min =< Max).

time_dependence(#state{}) ->
    dependent.

init({Out, Target, Interval, Drop, Min, Max}) ->
    NTarget = sbroker_util:sojourn_target(Target),
    NInterval = sbroker_util:interval(Interval),
    {Out, Drop, Min, Max, #state{target=NTarget, interval=NInterval,
                                 max_packet=Min}}.

%% To ensure following the reference codel implementationas closely as possible
%% use the full dequeue approach and "undo" the following:
%% * Changing first_above_time to undefined
%% * Changing from dropping true to false
%% This means that a slow queue is detected and items can be dropped but a
%% real dequeue is required to stop the first (or consecutive) slow intervals.
handle_timeout(Time, L, #state{first_above_time=undefined} = State) ->
    handle_out(Time, L, State);
handle_timeout(Time, L, #state{dropping=true, target=Target,
                               max_packet=MaxPacket,
                               first_above_time=FirstAbove} = State) ->
    {N, NState} = handle_out(Time, L, State),
    case lists:split(N, L) of
        {[], _} ->
            %% No items dropped so state does not change.
            {0, State};
        {_, [Sojourn | _] = NL}
          when Sojourn >= Target, length(NL) > MaxPacket ->
            %% Next item is slow, queue size is greater than max packet and item
            %% was not dropped so still dropping.
            {N, NState};
        {Dropped, _} ->
            %% Next item is below target, queue size is less than max packet or
            %% queue is empty, so dropping may have been set to false.
            %% Reverse any state changes that occured by observing this.
            Pad = lists:duplicate(MaxPacket + 1, Target),
            case handle_out(Time, Dropped ++ Pad, State) of
                {N, NState2} ->
                    {N, NState2};
                {M, _} when M == N+1 ->
                    NState2 = NState#state{count=NState#state.count+1,
                                           dropping=true,
                                           first_above_time=FirstAbove},
                    {N, control_law(NState#state.drop_next, NState2)}
            end
    end;
handle_timeout(Time, L, #state{dropping=false,
                               first_above_time=FirstAbove} = State) ->
    case handle_out(Time, L, State) of
        {0, NState} ->
            %% Head might be below target, queue size below max packet or empty
            %% queue, maintain previous first_above_time.
            {0, NState#state{first_above_time=FirstAbove}};
        {1, NState} ->
            %% End of first interval resulted in drop. If new head is below
            %% target, queue size is less than or equal to max packet
            %% first_above_time is reset, maintain previous. Dropping is always
            %% true after first drop.
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

handle_out_r(Time, L, #state{max_packet=MaxPacket} = State) ->
    case handle_timeout(Time, L, State) of
        {Drops, NState} when (length(L) - Drops) =< MaxPacket ->
            {Drops, NState#state{first_above_time=undefined}};
        {Drops, NState} ->
            {Drops, NState}
    end.

config_change(Time, {Out, Target, Interval, Drop, Min, Max},
              #state{first_above_time=FirstAbove,
                     drop_next=DropNext} = State) ->
    NTarget = sbroker_util:sojourn_target(Target),
    NInterval = sbroker_util:interval(Interval),
    NFirstAbove = reduce(FirstAbove, Time+NInterval),
    NDropNext = reduce(DropNext, Time+NInterval),
    NState = State#state{target=NTarget, interval=NInterval, max_packet=Min,
                         first_above_time=NFirstAbove, drop_next=NDropNext},
    {Out, Drop, Min, Max, NState}.

reduce(undefined, _) ->
    undefined;
reduce(Next, Min) ->
    min(Next, Min).

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
                 Count > 2 andalso Now - DropNext < 8 * Interval ->
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
    {{nodrop, empty}, [], State#state{first_above_time=undefined}};
do_dequeue([SojournTime | L], #state{target=Target} = State)
  when SojournTime < Target ->
    {{nodrop, SojournTime}, L, State#state{first_above_time=undefined}};
do_dequeue([SojournTime | NL] = L, #state{max_packet=MaxPacket} = State)
  when length(L) =< MaxPacket ->
    {{nodrop, SojournTime}, NL, State#state{first_above_time=undefined}};
do_dequeue([SojournTime | L], #state{interval=Interval, now=Now,
                                     first_above_time=undefined} = State) ->
    FirstAbove = Now + Interval,
    {{nodrop, SojournTime}, L, State#state{first_above_time=FirstAbove}};
do_dequeue([SojournTime | L], #state{first_above_time=FirstAbove,
                                     now=Now} = State)
  when Now >= FirstAbove ->
    {{drop, SojournTime}, L, State};
do_dequeue([SojournTime | L], State) ->
    {{nodrop, SojournTime}, L, State}.
