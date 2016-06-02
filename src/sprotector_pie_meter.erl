%%-------------------------------------------------------------------
%%
%% Copyright (c) 2016, James Fish <james@fishcakez.com>
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
%% @doc Registers the process with and updates the `sprotector_server' using the
%% "basic" PIE active queue management using approximate queue sojourn times.
%%
%% `sprotector_pie_meter' can be used as the `sbroker_meter' in a `sbroker' or
%% a `sregulator'. Its argument is of the form:
%% ```
%% {AskTarget :: non_neg_integer(), AskInterval :: pos_integer(),
%%  AskRTarget :: non_neg_integer(), AskInterval :: pos_integer(),
%%  Update :: pos_integer(), Min :: non_neg_integer(),
%%  Max :: non_neg_integer() | infinity}
%% '''
%% `AskTarget' is the target delay for the `ask' queue and `AskInterval' is the
%% initial interval before the first drop when the processes message queue is
%% above the minimum size `Min' and below the maximim `Max'. `AskRTarget' and
%% `AskRInterval' are equivalent for the `ask_r' queue. `Update' is the interval
%% between updating the drop probability.
%% @see sprotector
%% @see sprotector_server
%% @reference Rong Pan, Prem Natarajan, Chiara Piglione, Mythili Suryanarayana
%% Prabhu, Venkatachalam Subramanian, Fred Baker, and Bill Ver Steeg, PIE: A
%% lightweight control scheme to address the bufferbloat problem, 2013.
-module(sprotector_pie_meter).

-behaviour(sbroker_meter).

-export([init/2]).
-export([handle_update/5]).
-export([handle_info/3]).
-export([code_change/4]).
-export([config_change/3]).
-export([terminate/2]).

-record(pie, {target :: non_neg_integer(),
              interval :: pos_integer(),
              alpha :: integer(),
              beta :: integer(),
              allowance :: non_neg_integer(),
              drop = 0.0 :: float(),
              last :: undefined | non_neg_integer(),
              updated :: integer()}).

-record(state, {update :: non_neg_integer(),
                ask :: #pie{},
                bid :: #pie{},
                update_next :: integer()}).

%% @private
-spec init(Time,
           {AskTarget, AskInterval, AskRTarget, AskRInterval,
            Update, Min, Max}) -> {State, Time} when
      Time :: integer(),
      AskTarget :: non_neg_integer(),
      AskInterval :: pos_integer(),
      AskRTarget :: non_neg_integer(),
      AskRInterval :: pos_integer(),
      Update :: pos_integer(),
      Min :: non_neg_integer(),
      Max :: non_neg_integer() | infinity,
      State :: #state{}.
init(Time, {AskTarget, AskInterval, BidTarget, BidInterval,
            Update, Min, Max}) ->
    AskPie = init(Time, AskTarget, AskInterval),
    BidPie = init(Time, BidTarget, BidInterval),
    NUpdate = sbroker_util:interval(Update),
    true = sprotector_server:register(self(), Min, Max),
    State = #state{ask=AskPie, bid=BidPie, update=NUpdate, update_next=Time},
    {State, Time}.

%% @private
-spec handle_update(QueueDelay, ProcessDelay, RelativeTime, Time, State) ->
    {NState, Next} when
      QueueDelay :: non_neg_integer(),
      ProcessDelay :: non_neg_integer(),
      RelativeTime :: integer(),
      Time :: integer(),
      State :: #state{},
      NState :: #state{},
      Next :: integer().
handle_update(QueueDelay, _, RelativeTime, Time, State) ->
    AskSojourn = sojourn(QueueDelay, RelativeTime),
    BidSojourn = sojourn(QueueDelay, -RelativeTime),
    handle_update(AskSojourn, BidSojourn, Time, State).

%% @private
-spec handle_info(Msg, Time, State) -> {State, Next} when
      Msg :: any(),
      Time :: integer(),
      State :: #state{},
      Next :: integer().
handle_info(_, Time, State) ->
    handle(Time, State).

%% @private
-spec code_change(OldVsn, Time, State, Extra) -> {State, Next} when
      OldVsn :: any(),
      Time :: integer(),
      State :: #state{},
      Extra :: any(),
      Next :: integer().
code_change(_, Time, State, _) ->
    handle(Time, State).

%% @private
-spec config_change({AskTarget, AskInterval, AskRTarget, AskRInterval,
                     Update, Min, Max}, Time, State) -> {NState, Next} when
      Time :: integer(),
      AskTarget :: non_neg_integer(),
      AskInterval :: pos_integer(),
      AskRTarget :: non_neg_integer(),
      AskRInterval :: pos_integer(),
      Update :: pos_integer(),
      Min :: non_neg_integer(),
      Max :: non_neg_integer() | infinity,
      State :: #state{},
      NState :: #state{},
      Next :: integer().
config_change({AskTarget, AskInterval, BidTarget, BidInterval,
               Update, Min, Max}, Time,
              #state{ask=AskPie, bid=BidPie, update_next=UpdateNext}) ->
    {AskDrop, NAskPie} = change(AskTarget, AskInterval, AskPie),
    {BidDrop, NBidPie} = change(BidTarget, BidInterval, BidPie),
    NUpdate = sbroker_util:interval(Update),
    sprotector_server:change(self(), Min, Max, queue_len(), AskDrop, BidDrop),
    NUpdateNext = min(Time+NUpdate, max(Time, UpdateNext)),
    NState = #state{update=NUpdate, ask=NAskPie, bid=NBidPie,
                    update_next=NUpdateNext},
    {NState, NUpdateNext}.

%% @private
-spec terminate(Reason, State) -> true when
      Reason :: any(),
      State :: #state{}.
terminate(_, _) ->
    sprotector_server:unregister(self()).

%% Internal

init(Time, Target, Interval) ->
    Alpha = erlang:convert_time_unit(8, 1, native),
    Beta = erlang:convert_time_unit(4, 5, native),
    NTarget = sbroker_util:sojourn_target(Target),
    NInterval = sbroker_util:interval(Interval),
    #pie{target=NTarget, interval=NInterval, alpha=Alpha, beta=Beta,
         allowance=NInterval, updated=Time}.

sojourn(QueueDelay, RelativeTime) ->
    QueueDelay + max(0, RelativeTime).

handle_update(AskSojourn, BidSojourn, Time,
              #state{update_next=UpdateNext, ask=AskPie, bid=BidPie} = State)
  when UpdateNext > Time ->
    {AskDrop, NAskPie} = drop_control(AskSojourn, AskPie),
    {BidDrop, NBidPie} = drop_control(BidSojourn, BidPie),
    true = sprotector_server:update(self(), queue_len(), AskDrop, BidDrop),
    {State#state{ask=NAskPie, bid=NBidPie}, UpdateNext};
handle_update(AskSojourn, BidSojourn, Time,
              #state{update=Update, ask=AskPie, bid=BidPie} = State) ->
    {AskDrop, NAskPie} = update(AskSojourn, Time, AskPie),
    {BidDrop, NBidPie} = update(BidSojourn, Time, BidPie),
    true = sprotector_server:update(self(), queue_len(), AskDrop, BidDrop),
    UpdateNext = Time + Update,
    NState = State#state{ask=NAskPie, bid=NBidPie, update_next=UpdateNext},
    {NState, UpdateNext}.

update(Sojourn, Time, #pie{last=undefined} = Pie) ->
    update(Sojourn, Time, Pie#pie{last=Sojourn, updated=Time});
update(Sojourn, Time,
       #pie{alpha=Alpha, beta=Beta, target=Target, last=Last, drop=Drop,
            allowance=Allowance, updated=Updated} = Pie) ->
    NDrop = adjust((Sojourn - Target) / Alpha + (Sojourn - Last) / Beta, Drop),
    NDrop2 = decay(NDrop, Sojourn, Last),
    NPie = Pie#pie{last=Sojourn, updated=Time, drop=NDrop2,
                   allowance=min(0, Allowance-Updated)},
    drop_control(Sojourn, NPie).

adjust(Diff, Drop) ->
    max(0.0, min(1.0, Drop + scale(Diff, Drop))).

scale(Diff, Drop) when Drop < 0.000001 ->
    Diff / 2048;
scale(Diff, Drop) when Drop < 0.00001 ->
    Diff / 512;
scale(Diff, Drop) when Drop < 0.0001 ->
    Diff / 128;
scale(Diff, Drop) when Drop < 0.001 ->
    Diff / 32;
scale(Diff, Drop) when Drop < 0.01 ->
    Diff / 8;
scale(Diff, Drop) when Drop < 0.1 ->
    Diff / 2;
scale(Diff, _) ->
    Diff.

decay(Drop, 0, 0) ->
    Drop * 0.98;
decay(Drop, _, _) ->
    Drop.

drop_control(Sojourn,
          #pie{drop=0.0, last=Last, target=Target, interval=Interval,
               allowance=Allowance} = Pie)
  when Sojourn < Target div 2, Last < Target div 2 ->
    case Allowance of
        Interval ->
            {0.0, Pie};
        _ ->
            {0.0, Pie#pie{allowance=Allowance}}
    end;
drop_control(_, #pie{allowance=Allowance} = Pie) when Allowance > 0 ->
    {0.0, Pie};
drop_control(_, #pie{last=Last, target=Target, drop=Drop} = Pie)
  when Last < Target div 2, Drop < 0.2 ->
    {0.0, Pie};
drop_control(_, #pie{drop=Drop} = Pie) ->
    {Drop, Pie}.

change(Target, Interval, #pie{last=undefined} = Pie) ->
    NTarget = sbroker_util:sojourn_target(Target),
    NInterval = sbroker_util:interval(Interval),
    NPie = Pie#pie{target=NTarget, interval=NInterval, allowance=NInterval},
    {0.0, NPie};
change(Target, Interval, #pie{last=Last} = Pie) ->
    NTarget = sbroker_util:sojourn_target(Target),
    NInterval = sbroker_util:interval(Interval),
    NAllowance = change_allowance(NInterval, Pie),
    NPie = Pie#pie{target=NTarget, interval=NInterval, allowance=NAllowance},
    drop_control(Last, NPie).

change_allowance(_, #pie{allowance=0}) ->
    0;
change_allowance(NInterval, #pie{allowance=Allowance, interval=PrevInterval}) ->
    min(0, Allowance - PrevInterval + NInterval).

queue_len() ->
    {_, Len} = process_info(self(), message_queue_len),
    % Ignore '$mark' if present
    max(0, Len-1).

handle(Time, #state{update_next=UpdateNext} = State) ->
    {State, max(Time, UpdateNext)}.
