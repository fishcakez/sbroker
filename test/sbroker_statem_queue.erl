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

-export([init/2]).
-export([handle_in/4]).
-export([handle_out/2]).
-export([handle_timeout/2]).
-export([handle_cancel/3]).
-export([handle_info/3]).
-export([config_change/3]).
-export([to_list/1]).
-export([len/1]).
-export([terminate/2]).

-ifdef(LEGACY_TYPES).
-type internal_queue() :: queue().
-else.
-type internal_queue() :: queue:queue({integer(), {pid(), any()}, reference()}).
-endif.

-record(state, {config :: [non_neg_integer()],
                out :: out | out_r,
                drops :: [non_neg_integer()],
                queue = queue:new() :: internal_queue()}).

%% This sbroker_queue module takes a list of non_neg_integer() and drops the
%% integer at head of the list (or the whole queue if the queue length is
%% lower). The tail is kept and used for the next call. Once the list is emptied
%% the original list is used in its place, if this list is empty no drops
%% occcur.
%%
%% Time is ignored completely to allow testing independent of time in sbroker.
%% Timing is tested for separately using `sbroker_queue_statem`.

init(_, {Out, Drops}) ->
    #state{config=Drops, out=Out, drops=Drops}.

handle_in(SendTime, {Pid, _} = From, Time, State) ->
    #state{queue=Q} = NState = handle_timeout(Time, State),
    Ref = monitor(process, Pid),
    NState#state{queue=queue:in({SendTime, From, Ref}, Q)}.

handle_out(Time, #state{out=Out} = State) ->
    #state{queue=Q} = NState = handle_timeout(Time, State),
    case queue:Out(Q) of
        {empty, _} ->
            {empty, NState};
        {{value, {SendTime, From, Ref}}, NQ} ->
            demonitor(Ref, [flush]),
            {SendTime, From, NState#state{queue=NQ}}
    end.

%% If queue is empty don't change state.
handle_timeout(Time, #state{queue=Q} = State) ->
    case queue:is_empty(Q) of
        true ->
            State;
        false ->
            timeout(Time, State)
    end.

handle_cancel(Tag, Time, State) ->
    #state{queue=Q} = NState = handle_timeout(Time, State),
    Len = queue:len(Q),
    Cancel = fun({_, {_, Tag2}, Ref}) when Tag2 =:= Tag ->
                     demonitor(Ref, [flush]),
                     false;
                (_) ->
                     true
             end,
    NQ = queue:filter(Cancel, Q),
    case queue:len(NQ) of
        Len ->
            {false, NState};
        NLen ->
            {Len - NLen, NState#state{queue=NQ}}
    end.
handle_info({'DOWN', Ref, _, _, _}, Time, State) ->
    #state{queue=Q} = NState = handle_timeout(Time, State),
    NQ = queue:filter(fun({_, _, Ref2}) -> Ref2 =/= Ref end, Q),
    NState#state{queue=NQ};
handle_info(_, Time, State) ->
    handle_timeout(Time, State).

config_change({Out, Config}, Time, #state{config=Config} = State) ->
    handle_timeout(Time, State#state{out=Out});
config_change({Out, Config}, Time, State) ->
    handle_timeout(Time, State#state{config=Config, out=Out, drops=Config}).


to_list(#state{queue=Q}) ->
    [erlang:delete_element(3, Item) || Item <- queue:to_list(Q)].

len(#state{queue=Q}) ->
    queue:len(Q).

terminate(_, #state{queue=Q}) ->
    _ = [demonitor(Ref, [flush]) || {_, _, Ref} <- queue:to_list(Q)],
    ok.

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

drop_item(Time, {SendTime, From, Ref}) ->
    demonitor(Ref, [flush]),
    sbroker_queue:drop(From, SendTime, Time).
