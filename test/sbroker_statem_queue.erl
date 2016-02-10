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
-module(sbroker_statem_queue).

-behaviour(sbroker_queue).

-export([init/3]).
-export([handle_in/5]).
-export([handle_out/2]).
-export([handle_timeout/2]).
-export([handle_cancel/3]).
-export([handle_info/3]).
-export([config_change/3]).
-export([len/1]).
-export([terminate/2]).

-record(state, {config :: [non_neg_integer()],
                out :: out | out_r,
                drops :: [non_neg_integer()],
                queue :: sbroker_queue:internal_queue()}).

%% This sbroker_queue module takes a list of non_neg_integer() and drops the
%% integer at head of the list (or the whole queue if the queue length is
%% lower). The tail is kept and used for the next call. Once the list is emptied
%% the original list is used in its place, if this list is empty no drops
%% occcur.
%%
%% Time is ignored completely to allow testing independent of time in sbroker.
%% Timing is tested for separately using `sbroker_queue_statem`.

init(Q, Time, {Out, Drops}) ->
    handle_timeout(Time, #state{config=Drops, out=Out, drops=Drops, queue=Q}).

handle_in(SendTime, {Pid, _} = From, Value, Time, State) ->
    {#state{queue=Q} = NState, TimeoutNext} = handle_timeout(Time, State),
    Ref = monitor(process, Pid),
    NState2 = NState#state{queue=queue:in({SendTime, From, Value, Ref}, Q)},
    {NState2, TimeoutNext}.

handle_out(Time, #state{out=Out} = State) ->
    {#state{queue=Q} = NState, TimeoutNext} = handle_timeout(Time, State),
    case queue:Out(Q) of
        {empty, _} ->
            #state{config=Drops} = NState,
            {empty, NState#state{drops=Drops}};
        {{value, {SendTime, From, Value, Ref}}, NQ} ->
            {SendTime, From, Value, Ref, NState#state{queue=NQ}, TimeoutNext}
    end.

%% If queue is empty don't change state.
handle_timeout(Time, #state{queue=Q} = State) ->
    case queue:is_empty(Q) of
        true ->
            {State, infinity};
        false ->
            {timeout(Time, State), infinity}
    end.

handle_cancel(Tag, Time, State) ->
    {#state{queue=Q} = NState, TimeoutNext} = handle_timeout(Time, State),
    Len = queue:len(Q),
    Cancel = fun({_, {_, Tag2}, _, Ref}) when Tag2 =:= Tag ->
                     demonitor(Ref, [flush]),
                     false;
                (_) ->
                     true
             end,
    NQ = queue:filter(Cancel, Q),
    case queue:len(NQ) of
        Len ->
            {false, NState, TimeoutNext};
        NLen ->
            {Len - NLen, NState#state{queue=NQ}, TimeoutNext}
    end.

handle_info({'DOWN', Ref, _, _, _}, Time, State) ->
    {#state{queue=Q} = NState, TimeoutNext} = handle_timeout(Time, State),
    NQ = queue:filter(fun({_, _, _, Ref2}) -> Ref2 =/= Ref end, Q),
    {NState#state{queue=NQ}, TimeoutNext};
handle_info(_, Time, State) ->
    handle_timeout(Time, State).

config_change({Out, Config}, Time, #state{config=Config} = State) ->
    handle_timeout(Time, State#state{out=Out});
config_change({Out, Config}, Time, State) ->
    handle_timeout(Time, State#state{config=Config, out=Out, drops=Config}).

len(#state{queue=Q}) ->
    queue:len(Q).

terminate(_, #state{queue=Q}) ->
    Q.

%% Internal

timeout(_, #state{config=[], drops=[]} = State) ->
    State;
timeout(Time, #state{config=Config, drops=[]} = State) ->
    timeout(Time, State#state{drops=Config});
timeout(Time, #state{drops=[Drop | Drops], queue=Q} = State) ->
    Drop2 = min(Drop, queue:len(Q)),
    {DropQ, NQ} = queue:split(Drop2, Q),
    drop_queue(Time, DropQ),
    State#state{drops=Drops, queue=NQ}.

drop_queue(Time, Q) ->
    _ = [drop_item(Time, Item) || Item <- queue:to_list(Q)],
    ok.

drop_item(Time, {SendTime, From, _, Ref}) ->
    demonitor(Ref, [flush]),
    sbroker_queue:drop(From, SendTime, Time).
