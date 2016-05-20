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
-module(sbroker_fq_queue).

-behaviour(sbroker_queue).
-behaviour(sbroker_fair_queue).

-export([init/3]).
-export([handle_in/5]).
-export([handle_out/2]).
-export([handle_fq_out/2]).
-export([handle_timeout/2]).
-export([handle_cancel/3]).
-export([handle_info/3]).
-export([code_change/4]).
-export([config_change/3]).
-export([len/1]).
-export([terminate/2]).

-type action() :: {Drops :: non_neg_integer(),
                   TimeoutIncrement :: timeout()}.

-record(state, {config :: [action()],
                actions :: [action()],
                queue :: sbroker_queue:internal_queue()}).

%% This sbroker_queue module takes a list of tuples. The first element is a
%% non_neg_integer() that drops the integer at head of the list (or the whole
%% queue if the queue length is lower). The tail is kept and used for the next
%% call. Once the list is emptied the original list is used in its place. The
%% second element is a timeout increment which is added to the current time to
%% generate the next timeout time.
%%
%% Intended only for testing the sbroker_fair_queue

init(Q, Time, Actions) ->
    handle_timeout(Time, #state{config=Actions, actions=Actions, queue=Q}).

handle_in(SendTime, {Pid, _} = From, Value, Time, #state{queue=Q} = State) ->
    Ref = monitor(process, Pid),
    NState = State#state{queue=queue:in({SendTime, From, Value, Ref}, Q)},
    handle_timeout(Time, NState).

handle_out(Time, State) ->
    {#state{queue=Q} = NState, Timeout} = handle_timeout(Time, State),
    case queue:out(Q) of
        {empty, NQ} ->
            {empty, NState#state{queue=NQ}};
        {{value, {SendTime, From, Value, Ref}}, NQ} ->
            {SendTime, From, Value, Ref, NState#state{queue=NQ}, Timeout}
    end.

handle_fq_out(Time, State) ->
    case handle_out(Time, State) of
        {_, _, _, _, _, _} = Out ->
            Out;
        {empty, NState} ->
            {empty, NState, infinity}
    end.

handle_timeout(_, #state{config=[], actions=[]} = State) ->
    {State, infinity};
handle_timeout(Time, #state{config=Actions, actions=[]} = State) ->
    handle_timeout(Time, State#state{actions=Actions});
handle_timeout(Time,
               #state{actions=[{Drops, TimeoutIncr} | Actions],
                      queue=Q} = State) ->
    Drops2 = min(Drops, queue:len(Q)),
    {DropQ, NQ} = queue:split(Drops2, Q),
    drop_queue(Time, DropQ),
    NState = State#state{actions=Actions, queue=NQ},
    case TimeoutIncr of
        infinity ->
            {NState, infinity};
        _ ->
            {NState, Time+TimeoutIncr}
    end.

handle_cancel(Tag, Time, #state{queue=Q} = State) ->
    Len = queue:len(Q),
    Cancel = fun({_, {_, Tag2}, _, Ref}) when Tag2 =:= Tag ->
                     demonitor(Ref, [flush]),
                     false;
                (_) ->
                     true
             end,
    NQ = queue:filter(Cancel, Q),
    {NState, TimeoutNext} = handle_timeout(Time, State#state{queue=NQ}),
    case queue:len(NQ) of
        Len ->
            {false, NState, TimeoutNext};
        NLen ->
            {Len - NLen, NState, TimeoutNext}
    end.

handle_info({'DOWN', Ref, _, _, _}, Time, #state{queue=Q} = State) ->
    NQ = queue:filter(fun({_, _, _, Ref2}) -> Ref2 =/= Ref end, Q),
    handle_timeout(Time, State#state{queue=NQ});
handle_info(_, Time, State) ->
    handle_timeout(Time, State).

code_change(_, Time, State, _) ->
    handle_timeout(Time, State).

config_change(Config, Time, #state{config=Config} = State) ->
    handle_timeout(Time, State);
config_change(Config, Time, State) ->
    handle_timeout(Time, State#state{config=Config, actions=Config}).

len(#state{queue=Q}) ->
    queue:len(Q).

terminate(_, #state{queue=Q}) ->
    Q.

%% Internal

drop_queue(Time, Q) ->
    _ = [drop_item(Time, Item) || Item <- queue:to_list(Q)],
    ok.

drop_item(Time, {SendTime, From, _, Ref}) ->
    demonitor(Ref, [flush]),
    sbroker_queue:drop(From, SendTime, Time).
