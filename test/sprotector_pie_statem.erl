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
-module(sprotector_pie_statem).

-include_lib("proper/include/proper.hrl").

-export([module/0]).
-export([args/0]).
-export([init/2]).
-export([update_next/6]).
-export([update_post/6]).
-export([change/3]).
-export([timeout/2]).

-record(pie, {target,
              current,
              old,
              burst,
              burst_allowance,
              update,
              update_next,
              prev_update,
              drop_prob = 0.0}).

%     tar,    cur,    old,     burst   burst_allow up   up_n     prev_up  drop_prob
%{pie,2000000, 0,     4000000, 3000000,1000000, 3000000, -4000000,-7000000, 1.220703125e-6}
%
module() ->
    sprotector_pie_meter.

args() ->
    ?SUCHTHAT({_, _, _, _, _, Min, Max},
              {choose(0, 3), choose(1, 3), choose(0, 3), choose(1, 3),
               choose(1, 5), choose(0, 3), oneof([choose(0, 5), infinity])},
              Min =< Max).

init(Time, {AskTarget, AskBurst, BidTarget, BidBurst, Update, _, _}) ->
    {AskPie, Next} = pie_init(Time, AskTarget, AskBurst, Update),
    {BidPie, Next} = pie_init(Time, BidTarget, BidBurst, Update),
    {{AskPie, BidPie}, Next}.

update_next({AskPie, BidPie}, Time, _, QueueDelay, _, RelativeTime) ->
    AskSojourn = QueueDelay + max(0, RelativeTime),
    {_, NAskPie, Next} = pie_update(AskPie, Time, AskSojourn),
    BidSojourn = QueueDelay + max(0, -RelativeTime),
    {_, NBidPie, Next} = pie_update(BidPie, Time, BidSojourn),
    {{NAskPie, NBidPie}, Next}.

update_post({AskPie, BidPie}, Time, MsgQueueLen, QueueDelay, _, RelativeTime) ->
    AskSojourn = QueueDelay + max(0, RelativeTime),
    {AskDrop, _, Next} = pie_update(AskPie, Time, AskSojourn),
    BidSojourn = QueueDelay + max(0, -RelativeTime),
    {BidDrop, _, Next} = pie_update(BidPie, Time, BidSojourn),
    DropPost = drop_post(AskDrop, ask) andalso drop_post(BidDrop, ask_r),
    {DropPost andalso len_post(MsgQueueLen), Next}.

drop_post(ExpDrop, Queue) ->
    case sprotector_server:lookup(self(), ask) of
        Drop when Drop + 0.0005 > ExpDrop, Drop - 0.0005 < ExpDrop ->
            true;
        ObsDrop ->
            ct:pal("Drop ~p~nExpected: ~p~nObserved: ~p",
                   [Queue, ExpDrop, ObsDrop]),
            false
    end.

len_post(MsgQueueLen) ->
    ExpLen = max(MsgQueueLen-1, 0),
    case sprotector_server:len(self()) of
        ExpLen ->
            true;
        ObsLen ->
            ct:pal("Len~nExpected: ~p~nObserved: ~p", [ExpLen, ObsLen]),
            false
    end.

change({AskPie, BidPie}, Time,
       {AskTarget, AskBurst, BidTarget, BidBurst, Update, _, _}) ->
    {NAskPie, Next} = pie_change(AskPie, Time, AskTarget, AskBurst, Update),
    {NBidPie, Next} = pie_change(BidPie, Time, BidTarget, BidBurst, Update),
    {{NAskPie, NBidPie}, Next}.

timeout({AskPie, BidPie}, Time) ->
    min(pie_timeout(AskPie, Time), pie_timeout(BidPie, Time)).

%% Helpers

pie_init(Time, Target, Burst, Update) ->
    NBurst = sbroker_util:interval(Burst),
    NUpdate = sbroker_util:interval(Update),
    Pie = #pie{target=sbroker_util:sojourn_target(Target), burst=NBurst,
               burst_allowance=NBurst, update=NUpdate, update_next=Time},
    {Pie, Time}.

pie_update(#pie{old=undefined} = Pie, Time, Sojourn) ->
    NPie = Pie#pie{old=Sojourn, prev_update=Time},
    pie_update(NPie, Time, Sojourn);
pie_update(Pie, Time, Sojourn) ->
    {Drop, NPie} = enqueue(update(dequeue(Pie, Sojourn), Time)),
    {Drop, NPie, pie_timeout(NPie, Time)}.

dequeue(Pie, Sojourn) ->
    Pie#pie{current=Sojourn}.

update(#pie{drop_prob=DropProb, old=Old, target=Target,
            current=Current, burst_allowance=BurstAllow, update=Update,
            update_next=UpdateNext, prev_update=PrevUpdate} = Pie, Time)
  when UpdateNext =< Time ->
    Alpha = sbroker_time:convert_time_unit(8, 1, native),
    Beta = sbroker_time:convert_time_unit(4, 5, native),
    P = (Current-Target) / Alpha + (Current-Old) / Beta,

    NP = if
             DropProb < 0.000001 ->
                 P / 2048;
             DropProb < 0.00001 ->
                 P / 512;
             DropProb < 0.0001 ->
                 P / 128;
             DropProb < 0.001 ->
                 P / 32;
             DropProb < 0.01 ->
                 P / 8;
             DropProb < 0.1 ->
                 P / 2;
             true ->
                 P
         end,

    NDropProb = DropProb + NP,

    NDropProb2 = if
                     Current == 0.0, Old == 0.0 ->
                         NDropProb * 0.98;
                     true ->
                         NDropProb
                 end,

    NDropProb3 = max(0.0, min(1.0, NDropProb2)),

    Pie#pie{drop_prob=NDropProb3, old=Current, prev_update=Time,
            update_next=Time+Update,
            burst_allowance=max(0, BurstAllow-(Time-PrevUpdate))};
update(Pie, _) ->
    Pie.

enqueue(#pie{drop_prob=DropProb, current=Current, target=Target, old=Old,
             burst=Burst, burst_allowance=BurstAllow} = Pie) ->
    if
        DropProb == 0.0, Current < Target div 2, Old < Target div 2 ->
            {0.0, Pie#pie{burst_allowance=Burst}};
        DropProb < 0.2, Old < Target div 2 ->
            {0.0, Pie};
        BurstAllow > 0 ->
            {0.0, Pie};
        true ->
            {DropProb, Pie}
    end.

pie_change(#pie{current=undefined}, Time, Target, Burst, Update) ->
    pie_init(Time, Target, Burst, Update);
pie_change(#pie{burst_allowance=BurstAllow, burst=OldBurst,
                update_next=UpdateNext, current=Current} = Pie, Time, Target,
           Burst, Update) ->
    NTarget = sbroker_util:sojourn_target(Target),
    NBurst = sbroker_util:interval(Burst),
    NUpdate = sbroker_util:interval(Update),
    NUpdateNext = min(Time + NUpdate, UpdateNext),
    NBurstAllow = if
                      BurstAllow == 0 ->
                          0;
                      true ->
                          max(0, BurstAllow - OldBurst + NBurst)
                  end,
    NPie = Pie#pie{target=NTarget, burst_allowance=NBurstAllow,
                   burst=NBurst, update_next=NUpdateNext, update=NUpdate},
    {_, NPie2} = enqueue(dequeue(NPie, Current)),
    {NPie2, pie_timeout(NPie2, Time)}.

pie_timeout(#pie{old=undefined}, Time) ->
    Time;
pie_timeout(#pie{update_next=UpdateNext}, Time) ->
    max(Time, UpdateNext).
