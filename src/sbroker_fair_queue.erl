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
%% @doc Implements a fair queue containing multiple `sbroker_queue' queues.
%%
%% `sbroker_fair_queue' can be used as a `sbroker_queue' in a `sbroker' or
%% `sregulator'. It will provide a queue that enqueues requests to internal
%% queues based on an index and dequeues from the internal queues fairly. Its
%% argument, `spec()', is of the form:
%% ```
%% {Module :: module(), Args :: any(), Index :: index()}
%% '''
%% `Module' and `Args' are the module and arguments of the underlying
%% `sbroker_queue'. `Module' must implement the `sbroker_fair_queue' behaviour.
%%
%% `Index' is the method of choosing the queue to store a request in. To use the
%% application of the sender: `application'. If the application can not be
%% determined uses `undefined'. To use the node of the sender: `node'. To use
%% the pid of the sender: `pid'. To use the value of the request: `value'. To
%% use the Nth element of a tuple value: `{element, N}'. If the value is not a
%% tuple of at least size `N' uses `undefined'.
%%
%% One queue is used per key and so the number of possible keys should be
%% bounded. If the underlying queue limits the size of its queue this only
%% applies to the queue for that key. Requests with a different key will be in a
%% different queue and be part of a separate limit. To limit the number of
%% queues by hashing the key: `{hash, Index2, Range}', where `Index2' is any
%% `index()' except another hash and `Range' is the number of queues, from 1 to
%% 2^32.
%%
%% Queues are chosen using a simple round robin strategy. Request are dequeued
%% and empty queues removed using the `handle_fq_out/2' callback, instead of the
%% usual `handle_out/2':
%% ```
%% -callback handle_fq_out(Time :: integer(), State :: any()) ->
%%     {SendTime :: integer(), From :: {Sender :: pid(), Tag :: any()},
%%      Value:: any(), Ref :: reference, NState :: any(),
%%      TimeoutTime :: integer() | infinity} |
%%     {empty, NState :: any(), RemoveTime :: integer() | infinity}.
%% '''
%% The variables are equivalent to those of the `sbroker_queue' callback
%% `handle_out/2' with the addition of `RemoveTime', which is the time (or
%% `infinity' for never) to remove the empty queue from the fair queue for that
%% index.
%%
%% @reference John B. Nagle, On Packet Switches with Infinite Storage,
%% IEEE Transactions on Communications, vol. com-35, no. 4, April 1987.
-module(sbroker_fair_queue).

-behaviour(sbroker_queue).

%% public api

-export([init/3]).
-export([handle_in/5]).
-export([handle_out/2]).
-export([handle_timeout/2]).
-export([handle_cancel/3]).
-export([handle_info/3]).
-export([code_change/4]).
-export([config_change/3]).
-export([len/1]).
-export([send_time/1]).
-export([terminate/2]).

%% types

-type robin_queue(Index) :: queue:queue(Index).

-type key() :: application | node | pid | value | {element, pos_integer()}.
-type index() :: key() | {hash, key(), 1..32#4000000}.
-type spec() :: {Module :: module(), Args :: any(), Index :: index()}.

-export_type([key/0]).
-export_type([index/0]).
-export_type([spec/0]).

-callback handle_fq_out(Time :: integer(), State :: any()) ->
    {SendTime :: integer(), From :: {pid(), Tag :: any()}, Value :: any(),
     Ref :: reference(), NState :: any(), TimeoutTime :: integer() | infinity} |
    {empty, NState :: any(), RemoveTime :: integer() | infinity}.

-record(state, {module :: module(),
                index :: index(),
                args :: any(),
                robin = queue:new() :: robin_queue(Key),
                remove_time :: integer() | infinity,
                next :: integer() | infinity,
                empties :: #{Key => any()},
                queues :: #{Key => any()}}).

%% public API

%% @private
-spec init(Q, Time, {Module, Args, Index}) -> {State, TimeoutTime} when
      Q :: sbroker_queue:internal_queue(),
      Time :: integer(),
      Module :: module(),
      Args :: any(),
      Index :: index(),
      State :: #state{},
      TimeoutTime :: integer() | infinity.
init(Q, Time, {Module, Args, Index}) ->
    Index = index(Index),
    InternalQs = to_lists(Index, Q),
    {Qs, Robin, Next2} = from_lists(Time, Module, Args, infinity, InternalQs),
    State = #state{module=Module, index=Index, args=Args, robin=Robin,
                   remove_time=infinity, next=Next2, empties=#{}, queues=Qs},
    {State, Next2}.

%% @private
-spec handle_in(SendTime, From, Value, Time, State) ->
    {NState, TimeoutNext} when
      SendTime :: integer(),
      From :: {pid(), any()},
      Value :: any(),
      Time :: integer(),
      State :: #state{},
      NState :: #state{},
      TimeoutNext :: integer() | infinity.
handle_in(SendTime, {Pid, _} = From, Value, Time,
          #state{index=Index, queues=Qs} = State) ->
    QKey = index(Index, Pid, Value),
    {NRobin, NEs, NQ, QNext} = in(QKey, SendTime, From, Value, Time, State),
    {NQs, NNext} = timeout(QKey, NQ, QNext, Qs, Time, State),
    NState = State#state{queues=NQs, empties=NEs, robin=NRobin, next=NNext},
    {remove(Time, NState), NNext}.

%% @private
-spec handle_out(Time, State) ->
    {SendTime, From, Value, Ref, NState, TimeoutNext} | {empty, NState} when
      Time :: integer(),
      State :: #state{},
      SendTime :: integer(),
      From :: {pid(), any()},
      Value :: any(),
      Ref :: reference(),
      NState :: #state{},
      TimeoutNext :: integer() | infinity.
handle_out(Time,
           #state{module=Module, robin=Robin, empties=Es, queues=Qs} = State) ->
    out(queue:out(Robin), Time, Module, Es, Qs, State).

%% @private
-spec handle_timeout(Time, State) -> {NState, TimeoutNext} when
      Time :: integer(),
      State :: #state{},
      NState :: #state{},
      TimeoutNext :: integer() | infinity.
handle_timeout(Time, #state{next=Next} = State) when Time < Next ->
    {remove(Time, State), Next};
handle_timeout(Time, #state{module=Module, queues=Qs} = State) ->
    {NQs, Next} = map(fun(_, Q) -> Module:handle_timeout(Time, Q) end, Qs),
    {remove(Time, State#state{queues=NQs, next=Next}), Next}.

%% @private
-spec handle_cancel(Tag, Time, State) -> {Reply, NState, TimeoutNext} when
      Tag :: any(),
      Time :: integer(),
      State :: #state{},
      Reply :: false | pos_integer(),
      NState :: #state{},
      TimeoutNext :: integer() | infinity.
handle_cancel(Tag, Time, #state{module=Module, queues=Qs} = State) ->
    QList = maps:to_list(Qs),
    {Reply, NQs, Next} = cancel(QList, Module, Tag, Time, false, infinity, []),
    {Reply, remove(Time, State#state{queues=NQs, next=Next}), Next}.

%% @private
-spec handle_info(Msg, Time, State) -> {NState, TimeoutNext} when
      Msg :: any(),
      Time :: integer(),
      State :: #state{},
      NState :: #state{},
      TimeoutNext :: integer() | infinity.
handle_info(Msg, Time, #state{module=Module, queues=Qs, empties=Es} = State) ->
    {NQs, Next} = map(fun(_, Q) -> Module:handle_info(Msg, Time, Q) end, Qs),
    {NEs, RemoveTime} = empty_info(Module, Msg, Time, Es),
    NState = State#state{queues=NQs, empties=NEs, next=Next,
                         remove_time=RemoveTime},
    {NState, Next}.

%% @private
-spec code_change(OldVsn, Time, State, Extra) -> {NState, NextTimeout} when
      OldVsn :: any(),
      Time :: integer(),
      State :: #state{},
      Extra :: any(),
      NState :: #state{},
      NextTimeout :: integer() | infinity.
code_change(_, Time, #state{next=Next} = State, _) ->
    % Can only handle code changes for this module, sbroker/sregulator won't
    % pass a change for other modules.
    {State, max(Time, Next)}.

%% @private
-spec config_change({Module, Args, Index}, Time, State) ->
    {NState, TimeoutTime} when
      Module :: module(),
      Args :: any(),
      Index :: index(),
      Time :: integer(),
      State :: #state{},
      NState :: #state{},
      TimeoutTime :: integer() | infinity.
config_change({Module, Args, Index}, Time,
              #state{module=Module, index=Index} = State) ->
    Change = fun(_, Q) ->
                     Module:config_change(Args, Time, Q)
             end,
    change(Change, Module, Args, State);
config_change({Module2, Args, Index}, Time,
              #state{module=Module, index=Index} = State) ->
    Change = fun(_, Q) ->
                     InternalQ = Module:terminate(change, Q),
                     Module2:init(InternalQ, Time, Args)
             end,
    change(Change, Module2, Args, State);
config_change({_, _, _} = Arg, Time, State) ->
    init(terminate(change, State), Time, Arg).

%% @private
-spec len(State) -> Len when
      State :: #state{},
      Len :: non_neg_integer().
len(#state{module=Module, queues=Qs}) ->
    maps:fold(fun(_, Q, Len) -> Module:len(Q) + Len end, 0, Qs).

%% @private
-spec send_time(State) -> SendTime | empty when
      State :: #state{},
      SendTime :: integer().
send_time(#state{module=Module, queues=Qs}) ->
    maps:fold(fun(_, Q, SendTime) -> min(Module:send_time(Q), SendTime) end,
              empty, Qs).

%% @private
-spec terminate(Reason, State) -> InternalQ when
      Reason :: sbroker_handlers:reason(),
      State :: #state{},
      InternalQ :: sbroker_queue:internal_queue().
terminate(Reason, #state{module=Module, queues=Qs, empties=Es})
  when Reason == change; Reason == stop ->
    Terminate = fun(_, Q, Acc) ->
                        InternalQ = Module:terminate(Reason, Q),
                        queue:to_list(InternalQ) ++ Acc
                end,
    EmptyTerminate = fun(QKey, {Q, _}, Acc) ->
                             Terminate(QKey, Q, Acc)
                     end,
    QList = maps:fold(EmptyTerminate, maps:fold(Terminate, [], Qs), Es),
    queue:from_list(lists:sort(QList));
terminate(Reason, _) ->
    % If an internal queue failed then state changes might be lost for other
    % internal queues than ran immediately before than failure
    exit({state_inconsistency, Reason}).

%% Internal

index(Index) when is_atom(Index) ->
    case lists:member(Index, [application, node, pid, value]) of
        true ->
            Index;
        false ->
            error(badarg, [Index])
    end;
index({element, N} = Index) when is_integer(N), N > 0 ->
    Index;
index({hash, Index, Range})
  when is_integer(Range), Range >= 1, Range =< 32#4000000 ->
    {hash, index(Index), Range};
index(Other) ->
    error(badarg, [Other]).

index(application, Pid, _) ->
    case application:get_application(Pid) of
        {ok, App} -> App;
        undefined -> undefined
    end;
index(node, Pid, _) ->
    node(Pid);
index(pid, Pid, _) ->
    Pid;
index(value, _, Value) ->
    Value;
index({element, Elem}, _, Value) when tuple_size(Value) >= Elem ->
    element(Elem, Value);
index({element, _}, _, _) ->
    undefined;
index({hash, Index, Range}, Pid, Value)
  when is_atom(Index); element(1, Index) =/= hash ->
    erlang:phash2(index(Index, Pid, Value), Range).

to_lists(Index, InternalQ) ->
    InternalList = queue:to_list(InternalQ),
    split_list(InternalList, Index, #{}).

split_list([{_, {Pid, _}, Value, _} = Item | Rest], Index, InternalQs) ->
    QKey = index(Index, Pid, Value),
    case maps:find(QKey, InternalQs) of
        {ok, QList} ->
            NInternalQs = maps:put(QKey, [Item | QList], InternalQs),
            split_list(Rest, Index, NInternalQs);
        error ->
            NInternalQs = maps:put(QKey, [Item], InternalQs),
            split_list(Rest, Index, NInternalQs)
    end;
split_list([], _, InternalQs) ->
    maps:to_list(InternalQs).

from_lists(Time, Module, Args, Next, InternalQs) ->
    from_lists(InternalQs, Time, Module, Args, Next, [], #{}).

from_lists([{QKey, List} | Rest], Time, Module, Args, Next, QKeys, Qs) ->
    InternalQ = queue:reverse(queue:from_list(List)),
    {Q, QNext} = Module:init(InternalQ, Time, Args),
    NQs = maps:put(QKey, Q, Qs),
    from_lists(Rest, Time, Module, Args, min(QNext, Next), [QKey | QKeys], NQs);
from_lists([], _, _, _, Next, QKeys, Qs) ->
    {Qs, queue:from_list(QKeys), Next}.

in(QKey, SendTime, {Pid, _} = From, Value, Time,
   #state{robin=Robin, queues=Qs, empties=Es, module=Module, args=Args}) ->
    case find_queue(QKey, Qs, Es) of
        {ok, Q} ->
            {NQ, QNext} = Module:handle_in(SendTime, From, Value, Time, Q),
            {Robin, Es, NQ, QNext};
        {ok, Q, NEs} ->
            {NQ, QNext} = Module:handle_in(SendTime, From, Value, Time, Q),
            {queue:in(QKey, Robin), NEs, NQ, QNext};
        error ->
            Item = {SendTime, From, Value, monitor(process, Pid)},
            InternalQ = queue:from_list([Item]),
            {Q, QNext} = Module:init(InternalQ, Time, Args),
            {queue:in(QKey, Robin), Es, Q, QNext}
    end.

find_queue(QKey, Qs, Es) ->
    case maps:find(QKey, Qs) of
        {ok, _} = OK ->
            OK;
        error ->
            find_queue(QKey, Es)
    end.

find_queue(QKey, Es) ->
    case maps:find(QKey, Es) of
        {ok, {Q, _}} ->
            {ok, Q, maps:remove(QKey, Es)};
        error ->
            error
    end.

timeout(QKey, Q, QNext, Qs, Time, #state{next=Next}) when Time < Next ->
    {maps:put(QKey, Q, Qs), min(QNext, Next)};
timeout(QKey, Q, QNext, Qs, Time, #state{module=Module}) ->
    Timeout = fun(QKey2, _) when QKey2 == QKey ->
                      {Q, QNext};
                 (_, Q2) ->
                      Module:handle_timeout(Time, Q2)
              end,
    case maps:is_key(QKey, Qs) of
        true ->
            map(Timeout, Qs);
        false ->
            map(maps:to_list(Qs), Timeout, QNext, [{QKey, Q}])
    end.

out({{value, QKey}, Robin}, Time, Module, Es, Qs, State) ->
    {ok, Q} = maps:find(QKey, Qs),
    case Module:handle_fq_out(Time, Q) of
       {_, _, _, _, NQ, QNext} = Result ->
            NRobin = queue:in(QKey, Robin),
            {NQs, NNext} = timeout(QKey, NQ, QNext, Qs, Time, State),
            NState = State#state{robin=NRobin, empties=Es, queues=NQs,
                                 next=NNext},
            NState2 = remove(Time, NState),
            % Consecutive descending setelement is efficient
            NResult = setelement(6, Result, NNext),
            setelement(5, NResult, NState2);
        {empty, NQ, QRemoveTime} when QRemoveTime > Time->
            NQs = maps:remove(QKey, Qs),
            NEs = maps:put(QKey, {NQ, QRemoveTime}, Es),
            NRemoveTime = min(State#state.remove_time, QRemoveTime),
            NState = State#state{remove_time=NRemoveTime},
            out(queue:out(Robin), Time, Module, NEs, NQs, NState);
        {empty, NQ, _} ->
            Mod = State#state.module,
            _ = Mod:terminate(change, NQ),
            NQs = maps:remove(QKey, Qs),
            out(queue:out(Robin), Time, Module, Es, NQs, State)
    end;
out({empty, Robin}, Time, _, Es, Qs, State) when map_size(Qs) == 0 ->
    NState = State#state{robin=Robin, empties=Es, queues=Qs, next=infinity},
    {empty, remove(Time, NState)}.

map(Fun, Qs) ->
    map(maps:to_list(Qs), Fun, infinity, []).

map([{QKey, Q} | Rest], Fun, Next, Qs) ->
    {NQ, QNext} = Fun(QKey, Q),
    map(Rest, Fun, min(QNext, Next), [{QKey, NQ} | Qs]);
map([], _, Next, Qs) ->
    {maps:from_list(Qs), Next}.

empty_info(Mod, Msg, Time, Es) ->
    empty_info(maps:to_list(Es), Mod, Msg, Time, infinity, []).

empty_info([{QKey, {Q, QRemoveTime}} | Rest], Mod, Msg, Time, RemoveTime, Es)
  when QRemoveTime > Time ->
    {NQ, _} = Mod:handle_info(Msg, Time, Q),
    NRemoveTime = min(QRemoveTime, RemoveTime),
    NEs = [{QKey, {NQ, QRemoveTime}} | Es],
    empty_info(Rest, Mod, Msg, Time, NRemoveTime, NEs);
empty_info([{_, {Q, _}} | Rest], Mod, Msg, Time, RemoveTime, Es) ->
    _ = Mod:terminate(change, Q),
    empty_info(Rest, Mod, Msg, Time, RemoveTime, Es);
empty_info([], _, _, _, RemoveTime, Es) ->
    {maps:from_list(Es), RemoveTime}.

remove(Time, #state{remove_time=RemoveTime} = State) when RemoveTime > Time ->
    State;
remove(Time, #state{module=Mod, empties=Es} = State) ->
    {NEs, RemoveTime} = do_remove(Mod, Time, Es),
    State#state{empties=NEs, remove_time=RemoveTime}.

do_remove(Mod, Time, Es) ->
    do_remove(maps:to_list(Es), Mod, Time, infinity, []).

do_remove([{_, {_, QRemoveTime}} = Item | Rest], Mod, Time, RemoveTime, Es)
  when QRemoveTime > Time ->
    do_remove(Rest, Mod, Time, min(QRemoveTime, RemoveTime), [Item | Es]);
do_remove([{_, {Q, _}} | Rest], Mod, Time, RemoveTime, Es) ->
    _ = Mod:terminate(chance, Q),
    do_remove(Rest, Mod, Time, RemoveTime, Es);
do_remove([], _, _, RemoveTime, Es) ->
    {maps:from_list(Es), RemoveTime}.

cancel([{QKey, Q} | Rest], Module, Tag, Time, Reply, Next, Qs) ->
    {QReply, NQ, QNext} = Module:handle_cancel(Tag, Time, Q),
    NReply = cancel_reply(Reply, QReply),
    NQs = [{QKey, NQ} | Qs],
    cancel(Rest, Module, Tag, Time, NReply, min(QNext, Next), NQs);
cancel([], _, _, _, Reply, Next, Qs) ->
    {Reply, maps:from_list(Qs), Next}.

cancel_reply(false, QReply) ->
    QReply;
cancel_reply(Reply, false) ->
    Reply;
cancel_reply(Reply, QReply) ->
    Reply + QReply.

change(Change, Module, Args,
       #state{queues=Qs, empties=Es, robin=Robin} = State) ->
    NRobin = queue:join(Robin, queue:from_list(maps:keys(Es))),
    {NQs, Next} = change_map(Change, Qs, Es),
    NState = State#state{module=Module, args=Args, queues=NQs, empties=#{},
                         robin=NRobin, next=Next, remove_time=infinity},
    {NState, Next}.

change_map(Fun, Qs, Es) ->
    NEs = [{QKey, Q} || {QKey, {Q, _}} <- maps:to_list(Es)],
    map(NEs ++ maps:to_list(Qs), Fun, infinity, []).
