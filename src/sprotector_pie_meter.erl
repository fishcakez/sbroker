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
%% `sprotector_pie_meter' can be used as a `sbroker_meter' in a `sbroker' or
%% a `sregulator'. It will provide a short circuiter that will drop requests
%% without messaging the broker or regulator with a probability using PIE for
%% the `ask' and `ask_r' queue when the message queue length is approximately
%% above a minimum and all requests when it is approximately above a maximum.
%% Its argument, `spec()', is of the form:
%% ```
%% #{ask    => #{target   => AskTarget :: non_neg_integer(), % default: 100
%%               interval => AskInterval :: pos_integer()}, % default: 1000
%%   ask_r  => #{target   => AskRTarget :: non_neg_integer(), % default: 100
%%               interval => AskRInterval :: pos_integer()}, % default: 1000
%%   update => Update :: pos_integer(), % default: 100
%%   min    => Min :: non_neg_integer(), % default: 0
%%   max    => Max :: non_neg_integer() | infinity}. % default: infinity
%% '''
%% `AskTarget' is the target delay in milliseconds (defaults to `100') for the
%% `ask' queue and `AskInterval' is the initial interval in milliseconds
%% (defaults to `1000') before the first drop when the process's message queue
%% is above the minimum size `Min' (defaults to `0') and below the maximum size
%% `Max' (defaults to `infinity). `AskRTarget' and `AskRInterval' are equivalent
%% for the `ask_r' queue. `Update' is the interval between updating the drop
%% probability (defaults to `100').
%%
%% A person perceives a response time of `100' milliseconds or less as
%% instantaneous, and feels engaged with a system if response times are `1000'
%% milliseconds or less. Therefore it is desirable for a system to respond
%% within `1000' milliseconds as a worst case (upper percentile response time)
%% and ideally to respond within `100' milliseconds (target response time). Thus
%% the default target is `100' milliseconds and the default is interval `1000'
%% milliseconds.
%%
%% @see sprotector
%% @see sprotector_server
%% @reference Rong Pan, Prem Natarajan, Chiara Piglione, Mythili Suryanarayana
%% Prabhu, Venkatachalam Subramanian, Fred Baker, and Bill Ver Steeg, PIE: A
%% lightweight control scheme to address the bufferbloat problem, 2013.
%% @reference Stuart Card, George Robertson and Jock Mackinlay, The Information
%% Visualizer: An Information Workspace, ACM Conference on Human Factors in
%% Computing Systems, 1991.
-module(sprotector_pie_meter).

-behaviour(sbroker_meter).

-export([init/2]).
-export([handle_update/5]).
-export([handle_info/3]).
-export([code_change/4]).
-export([config_change/3]).
-export([terminate/2]).

%% types

-type spec() ::
    #{ask    => #{target   => AskTarget :: non_neg_integer(),
                  interval => AskInterval :: pos_integer()},
      ask_r  => #{target   => AskRTarget :: non_neg_integer(),
                  interval => AskRInterval :: pos_integer()},
      update => Update :: pos_integer(),
      min    => Min :: non_neg_integer(),
      max    => Max :: non_neg_integer() | infinity}.

-export_type([spec/0]).

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
-spec init(Time, Spec) -> {State, Time} when
      Time :: integer(),
      Spec :: spec(),
      State :: #state{}.
init(Time, Spec) ->
    AskPie = init(Time, ask, Spec),
    BidPie = init(Time, ask_r, Spec),
    NUpdate = sbroker_util:update(Spec),
    {Min, Max} = sbroker_util:min_max(Spec),
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
-spec config_change(Spec, Time, State) -> {NState, Next} when
      Spec :: spec(),
      Time :: integer(),
      State :: #state{},
      NState :: #state{},
      Next :: integer().
config_change(Spec, Time,
              #state{ask=AskPie, bid=BidPie, update_next=UpdateNext}) ->
    {AskDrop, NAskPie} = change(ask, Spec, AskPie),
    {BidDrop, NBidPie} = change(ask_r, Spec, BidPie),
    Update = sbroker_util:update(Spec),
    {Min, Max} = sbroker_util:min_max(Spec),
    sprotector_server:change(self(), Min, Max, queue_len(), AskDrop, BidDrop),
    NUpdateNext = min(Time+Update, max(Time, UpdateNext)),
    NState = #state{update=Update, ask=NAskPie, bid=NBidPie,
                    update_next=NUpdateNext},
    {NState, NUpdateNext}.

%% @private
-spec terminate(Reason, State) -> true when
      Reason :: any(),
      State :: #state{}.
terminate(_, _) ->
    sprotector_server:unregister(self()).

%% Internal

init(Time, Queue, Spec) ->
    Alpha = erlang:convert_time_unit(8, 1, native),
    Beta = erlang:convert_time_unit(4, 5, native),
    Target = sbroker_util:sojourn_target(Queue, Spec),
    Interval = sbroker_util:interval(Queue, Spec),
    #pie{target=Target, interval=Interval, alpha=Alpha, beta=Beta,
         allowance=Interval, updated=Time}.

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
                   allowance=max(0, Allowance-(Time-Updated))},
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
            {0.0, Pie#pie{allowance=Interval}}
    end;
drop_control(_, #pie{allowance=Allowance} = Pie) when Allowance > 0 ->
    {0.0, Pie};
drop_control(_, #pie{last=Last, target=Target, drop=Drop} = Pie)
  when Last < Target div 2, Drop < 0.2 ->
    {0.0, Pie};
drop_control(_, #pie{drop=Drop} = Pie) ->
    {Drop, Pie}.

change(Queue, Spec, #pie{last=undefined} = Pie) ->
    Target = sbroker_util:sojourn_target(Queue, Spec),
    Interval = sbroker_util:interval(Queue, Spec),
    NPie = Pie#pie{target=Target, interval=Interval, allowance=Interval},
    {0.0, NPie};
change(Queue, Spec, #pie{last=Last} = Pie) ->
    Target = sbroker_util:sojourn_target(Queue, Spec),
    Interval = sbroker_util:interval(Queue, Spec),
    Allowance = change_allowance(Interval, Pie),
    NPie = Pie#pie{target=Target, interval=Interval, allowance=Allowance},
    drop_control(Last, NPie).

change_allowance(_, #pie{allowance=0}) ->
    0;
change_allowance(Interval, #pie{allowance=Allowance, interval=PrevInterval}) ->
    max(0, Allowance - PrevInterval + Interval).

queue_len() ->
    {_, Len} = process_info(self(), message_queue_len),
    % Ignore '$mark' if present
    max(0, Len-1).

handle(Time, #state{update_next=UpdateNext} = State) ->
    {State, max(Time, UpdateNext)}.
