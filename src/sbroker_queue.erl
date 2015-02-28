%%-------------------------------------------------------------------
%%
%% Copyright (c) 2015, James Fish <james@fishcakez.com>
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
%% @private
-module(sbroker_queue).

%% public api

-export([new/2]).
-export([in/3]).
-export([out/2]).
-export([cancel/3]).
-export([down/3]).
-export([config_change/2]).
-export([timeout/2]).

%% types

-record(drop_queue, {module :: module(),
                     args :: any(),
                     out :: out | out_r,
                     drop_out :: out | out_r,
                     size :: non_neg_integer() | infinity,
                     len = 0 :: non_neg_integer(),
                     squeue :: squeue:squeue()}).

-type tag() :: any().
-type spec() :: {module(), any(), out | out_r, non_neg_integer() | infinity,
                 drop | drop_r}.
-opaque drop_queue() :: #drop_queue{}.

-export_type([spec/0]).
-export_type([drop_queue/0]).

%% public api

-spec new(Time, Spec) -> Q when
      Time :: non_neg_integer(),
      Spec :: spec(),
      Q :: drop_queue().
new(Time, {Mod, Args, Out, Size, Drop})
  when (Out =:= out orelse Out =:= out_r) andalso
       ((is_integer(Size) andalso Size >= 0) orelse Size =:= infinity) ->
    #drop_queue{module=Mod, args=Args, out=Out, drop_out=drop_out(Drop),
                size=Size, squeue=squeue:new(Time, Mod, Args)}.

-spec in(Time, From, Q) -> NQ when
      Time :: non_neg_integer(),
      From :: {pid(), tag()},
      Q :: drop_queue(),
      NQ :: drop_queue().
in(Time, {Pid, _} = From,
   #drop_queue{drop_out=DropOut, size=Size, len=Len, squeue=S} = Q) ->
    Ref = monitor(process, Pid),
    {Drops, NS} = squeue:in(Time, {Ref, From}, S),
    case Len - drops(Drops) + 1 of
        NLen when NLen > Size ->
            {Dropped, NS2} = drop_out(DropOut, NLen - Size, NS),
            Q#drop_queue{len=NLen-Dropped, squeue=NS2};
        NLen ->
            Q#drop_queue{len=NLen, squeue=NS}
    end.

-spec out(Time, Q) -> {Result, NQ} when
      Time :: non_neg_integer(),
      Q :: drop_queue(),
      Result :: empty | {SojournTime, {Ref, From}},
      SojournTime :: non_neg_integer(),
      Ref ::  reference(),
      From :: {pid(), tag()},
      NQ :: drop_queue().
out(Time, #drop_queue{out=Out, len=Len, squeue=S} = Q) ->
    case squeue:Out(Time, S) of
        {empty, Drops, NS2} ->
            {empty, maybe_drop(Q#drop_queue{len=Len-drops(Drops), squeue=NS2})};
        {Item, Drops, NS2} ->
            {Item, maybe_drop(Q#drop_queue{len=Len-drops(Drops)-1, squeue=NS2})}
    end.

-spec cancel(Time, Tag, Q) -> {Cancelled, NQ} when
      Time :: non_neg_integer(),
      Tag :: tag(),
      Q :: drop_queue(),
      Cancelled :: pos_integer() | false,
      NQ :: drop_queue().
cancel(Time, Tag, #drop_queue{len=Len, squeue=S} = Q) ->
    Cancel = fun({Ref, {_, Tag2}}) when Tag2 =:= Tag ->
                     demonitor(Ref, [flush]),
                     false;
                (_) ->
                     true
             end,
    {Drops, NS} = squeue:filter(Time, Cancel, S),
    Dropped = drops(Drops),
    NLen = squeue:len(NS),
    NQ = maybe_drop(Q#drop_queue{len=NLen, squeue=NS}),
    case Len - Dropped - NLen of
        0 ->
            {false, NQ};
        N ->
            {N, NQ}
    end.

-spec down(Time, Ref, Q) -> NQ when
      Time :: non_neg_integer(),
      Ref :: reference(),
      Q :: drop_queue(),
      NQ :: drop_queue().
down(Time, Ref, #drop_queue{squeue=S} = Q) ->
    {Drops, NS} = squeue:filter(Time, fun({Ref2, _}) -> Ref2 =/= Ref end, S),
    _ = drops(Drops),
    maybe_drop(Q#drop_queue{len=squeue:len(NS), squeue=NS}).


-spec config_change(Spec, Q) -> NQ when
      Spec :: spec(),
      Q :: drop_queue(),
      NQ :: drop_queue().
config_change({Mod, Args, Out, Size, Drop},
              #drop_queue{module=Mod, args=Args} = Q) ->
    Q#drop_queue{out=Out, size=Size, drop_out=drop_out(Drop)};
config_change(Spec, #drop_queue{len=Len, squeue=S}) ->
    Time = squeue:time(S),
    #drop_queue{squeue=NS} = NQ = new(Time, Spec),
    NQ#drop_queue{len=Len, squeue=squeue:join(NS, S)}.

-spec timeout(Time, Q) -> NQ when
      Time :: non_neg_integer(),
      Q :: drop_queue(),
      NQ :: drop_queue().
timeout(Time, #drop_queue{len=Len, squeue=S} = Q) ->
    {Drops, NS} = squeue:timeout(Time, S),
    maybe_drop(Q#drop_queue{len=Len-drops(Drops), squeue=NS}).

%% Internal

drop_out(drop) -> out;
drop_out(drop_r) -> out_r.

drop_out(DropOut, ToDrop, S) ->
    drop_loop(squeue:DropOut(S), DropOut, ToDrop, 0).

drop_loop({empty, Drops, S}, _, _, Dropped) ->
    {Dropped + drops(Drops), S};
drop_loop({Item, Drops, S}, DropOut, ToDrop, Dropped) ->
    drop(Item),
    case Dropped + 1 + drops(Drops) of
        NDropped when NDropped < ToDrop ->
            drop_loop(squeue:DropOut(S), DropOut, ToDrop, NDropped);
        NDropped ->
            {NDropped, S}
    end.

drops(Items) ->
    drops(Items, 0).

drops([Item | Rest], N) ->
    drop(Item),
    drops(Rest, N+1);
drops([], N) ->
    N.

drop({SojournTime, {Ref, From}}) ->
    demonitor(Ref, [flush]),
    gen_fsm:reply(From, {drop, SojournTime}).

maybe_drop(#drop_queue{size=Size, len=Len, drop_out=DropOut, squeue=S} = Q)
  when Len > Size ->
    {Dropped, NS} = drop_out(DropOut, Len - Size, S),
    Q#drop_queue{len=Len-Dropped, squeue=NS};
maybe_drop(Q) ->
    Q.
