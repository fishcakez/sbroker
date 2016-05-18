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
%% Its argument is of the form:
%% ```
%% {Module :: module(), Args :: any(), Index :: index()}
%% '''
%% `Module' and `Args' are the module and arguments of the underlying
%% `sbroker_queue' with pure `init/3' and `terminate/2' callbacks.
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
%% Queues are chosen using a simple round robin strategy and empty queues are
%% removed completely.
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
-export([config_change/3]).
-export([len/1]).
-export([terminate/2]).

%% types

-type robin_queue(Index) :: queue:queue(Index).

-type key() :: application | node | pid | value | {element, pos_integer()}.
-type index() :: key() | {hash, key(), 1..32#4000000}.

-export_type([key/0]).
-export_type([index/0]).

-record(state, {module :: module(),
                index :: index(),
                empty :: any(),
                robin = queue:new() :: robin_queue(Key),
                next :: integer() | infinity,
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
    {Empty, Next} = Module:init(queue:new(), Time, Args),
    InternalQs = to_lists(Index, Q),
    {Qs, Robin, Next2} = from_lists(Time, Module, Args, Next, InternalQs),
    State = #state{module=Module, index=Index, empty=Empty,
                   robin=Robin, next=Next2, queues=Qs},
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
          #state{module=Module, index=Index} = State) ->
    QKey = index(Index, Pid, Value),
    {Q, NRobin} = get_queue(QKey, State),
    {NQ, QNext} = Module:handle_in(SendTime, From, Value, Time, Q),
    {NQs, NNext} = timeout(QKey, NQ, QNext, Time, State),
    {State#state{queues=NQs, robin=NRobin, next=NNext}, NNext}.

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
handle_out(Time, #state{module=Module, robin=Robin, queues=Qs} = State) ->
    out(queue:out(Robin), Time, Module, Qs, State).

%% @private
-spec handle_timeout(Time, State) -> {NState, TimeoutNext} when
      Time :: integer(),
      State :: #state{},
      NState :: #state{},
      TimeoutNext :: integer() | infinity.
handle_timeout(Time, #state{next=Next} = State) when Time < Next ->
    {State, Next};
handle_timeout(Time, #state{module=Module, queues=Qs} = State) ->
    {NQs, Next} = map(fun(_, Q) -> Module:handle_timeout(Time, Q) end, Qs),
    {State#state{queues=NQs, next=Next}, Next}.

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
    {Reply, State#state{queues=NQs, next=Next}, Next}.

%% @private
-spec handle_info(Msg, Time, State) -> {NState, TimeoutNext} when
      Msg :: any(),
      Time :: integer(),
      State :: #state{},
      NState :: #state{},
      TimeoutNext :: integer() | infinity.
handle_info(Msg, Time, #state{module=Module, queues=Qs} = State) ->
    {NQs, Next} = map(fun(_, Q) -> Module:handle_info(Msg, Time, Q) end, Qs),
    {State#state{queues=NQs, next=Next}, Next}.

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
              #state{module=Module, index=Index, queues=Qs} = State) ->
    {Empty, Next} = Module:init(queue:new(), Time, Args),
    Change = fun(_, Q) ->
                     sbroker_queue:config_change(Module, Args, Time, Q)
             end,
    {NQs, Next2} = map(Change, Qs),
    Next3 = min(Next, Next2),
    {State#state{empty=Empty, queues=NQs, next=Next3}, Next3};
config_change({Module2, Args, Index}, Time,
              #state{module=Module, index=Index, queues=Qs} = State) ->
    {Empty, Next} = Module2:init(queue:new(), Time, Args),
    Change = fun(_, Q) ->
                     InternalQ = sbroker_queue:terminate(Module, change, Q),
                     sbroker_queue:init(Module2, InternalQ, Time, Args)
             end,
    {NQs, Next2} = map(Change, Qs),
    Next3 = min(Next, Next2),
    {State#state{module=Module2, empty=Empty, queues=NQs, next=Next3}, Next3};
config_change({_, _, _} = Arg, Time, State) ->
    init(terminate(change, State), Time, Arg).

%% @private
-spec len(State) -> Len when
      State :: #state{},
      Len :: non_neg_integer().
len(#state{module=Module, queues=Qs}) ->
    maps:fold(fun(_, Q, Len) -> Module:len(Q) + Len end, 0, Qs).

%% @private
-spec terminate(Reason, State) -> InternalQ when
      Reason :: sbroker_handlers:reason(),
      State :: #state{},
      InternalQ :: sbroker_queue:internal_queue().
terminate(Reason, #state{module=Module, queues=Qs}) ->
    Terminate = fun(_, Q, Acc) ->
                        InternalQ = sbroker_queue:terminate(Module, Reason, Q),
                        queue:to_list(InternalQ) ++ Acc
                end,
    QList = maps:fold(Terminate, [], Qs),
    queue:from_list(lists:sort(QList)).

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
    QList = maps:get(QKey, InternalQs, []),
    NInternalQs = maps:put(QKey, [Item | QList], InternalQs),
    split_list(Rest, Index, NInternalQs);
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

get_queue(QKey, #state{robin=Robin, empty=Empty, queues=Qs}) ->
    case maps:find(QKey, Qs) of
        {ok, Q} ->
            {Q, Robin};
        error ->
            {Empty, queue:in(QKey, Robin)}
    end.

timeout(QKey, Q, QNext, Time, #state{next=Next, queues=Qs}) when Time < Next ->
    {maps:put(QKey, Q, Qs), min(QNext, Next)};
timeout(QKey, Q, QNext, Time, #state{module=Module, queues=Qs}) ->
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

out({{value, QKey}, Robin}, Time, Module, Qs, State) ->
    {ok, Q} = maps:find(QKey, Qs),
    case Module:handle_out(Time, Q) of
       {_, _, _, _, NQ, QNext} = Result ->
            NRobin = queue:in(QKey, Robin),
            {NQs, NNext} = timeout(QKey, NQ, QNext, Time, State),
            NState = State#state{robin=NRobin, queues=NQs, next=NNext},
            % Consecutive descending setelement is efficient
            NResult = setelement(6, Result, NNext),
            setelement(5, NResult, NState);
        {empty, _} ->
            NQs = maps:remove(QKey, Qs),
            out(queue:out(Robin), Time, Module, NQs, State)
    end;
out({empty, Robin}, _, _, Qs, State) when map_size(Qs) == 0 ->
    {empty, State#state{robin=Robin, queues=Qs, next=infinity}}.

map(Fun, Qs) ->
    map(maps:to_list(Qs), Fun, infinity, []).

map([{QKey, Q} | Rest], Fun, Next, Qs) ->
    {NQ, QNext} = Fun(QKey, Q),
    map(Rest, Fun, min(QNext, Next), [{QKey, NQ} | Qs]);
map([], _, Next, Qs) ->
    {maps:from_list(Qs), Next}.

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
