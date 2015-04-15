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
-module(sbroker_statem).

%% ask_r is replaced by bid to help separate the difference between ask and
%% ask_r.

-include_lib("proper/include/proper.hrl").

-export([quickcheck/0]).
-export([quickcheck/1]).
-export([check/1]).
-export([check/2]).

-export([initial_state/0]).
-export([command/1]).
-export([precondition/2]).
-export([next_state/3]).
-export([postcondition/3]).
-export([cleanup/1]).

-export([start_link/1]).
-export([init/1]).
-export([spawn_client/2]).
-export([cancel/2]).
-export([change_config/2]).
-export([shutdown_client/2]).

-export([client_init/2]).

-record(state, {sbroker, asks=[], ask_out, ask_drops, ask_state=[], ask_size,
                ask_drop, bids=[], bid_out, bid_drops, bid_state=[], bid_size,
                bid_drop, cancels=[], done=[]}).

-define(TIMEOUT, 5000).

quickcheck() ->
    quickcheck([]).

quickcheck(Opts) ->
    proper:quickcheck(prop_sbroker(), Opts).

check(CounterExample) ->
    check(CounterExample, []).

check(CounterExample, Opts) ->
    proper:check(prop_sbroker(), CounterExample, Opts).

prop_sbroker() ->
    ?FORALL(Cmds, commands(?MODULE),
            ?TRAPEXIT(begin
                          {History, State, Result} = run_commands(?MODULE, Cmds),
                          cleanup(State),
                          ?WHENFAIL(begin
                                        io:format("History~n~p", [History]),
                                        io:format("State~n~p", [State]),
                                        io:format("Result~n~p", [Result])
                                    end,
                                    aggregate(command_names(Cmds), Result =:= ok))
                      end)).


initial_state() ->
    #state{}.

command(#state{sbroker=undefined} = State) ->
    {call, ?MODULE, start_link, start_link_args(State)};
command(State) ->
    frequency([{10, {call, ?MODULE, spawn_client,
                     spawn_client_args(State)}},
               {4, {call, sbroker, timeout, timeout_args(State)}},
               {3, {call, ?MODULE, cancel, cancel_args(State)}},
               {2, {call, ?MODULE, change_config,
                    change_config_args(State)}},
               {2, {call, ?MODULE, shutdown_client,
                    shutdown_client_args(State)}}]).

precondition(State, {call, _, start_link, Args}) ->
    start_link_pre(State, Args);
precondition(#state{sbroker=undefined}, _) ->
    false;
precondition(_State, _Call) ->
    true.

next_state(State, Value, {call, _, start_link, Args}) ->
    start_link_next(State, Value, Args);
next_state(State, Value, {call, _, spawn_client, Args}) ->
    spawn_client_next(State, Value, Args);
next_state(State, Value, {call, _, timeout, Args}) ->
    timeout_next(State, Value, Args);
next_state(State, Value, {call, _, cancel, Args}) ->
    cancel_next(State, Value, Args);
next_state(State, Value, {call, _, change_config, Args}) ->
    change_config_next(State, Value, Args);
next_state(State, Value, {call, _, shutdown_client, Args}) ->
    shutdown_client_next(State, Value, Args);
next_state(State, _Value, _Call) ->
    State.

postcondition(State, {call, _, start_link, Args}, Result) ->
    start_link_post(State, Args, Result);
postcondition(State, {call, _, spawn_client, Args}, Result) ->
    spawn_client_post(State, Args, Result);
postcondition(State, {call, _, timeout, Args}, Result) ->
    timeout_post(State, Args, Result);
postcondition(State, {call, _, cancel, Args}, Result) ->
    cancel_post(State, Args, Result);
postcondition(State, {call, _, change_config, Args}, Result) ->
    change_config_post(State, Args, Result);
postcondition(State, {call, _, shutdown_client, Args}, Result) ->
    shutdown_client_post(State, Args, Result);
postcondition(_State, _Call, _Result) ->
    true.

cleanup(#state{sbroker=undefined}) ->
    application:unset_env(sbroker, ?MODULE);
cleanup(#state{sbroker=Broker}) ->
    application:unset_env(sbroker, ?MODULE),
    Trap = process_flag(trap_exit, true),
    exit(Broker, shutdown),
    receive
        {'EXIT', Broker, shutdown} ->
            _ = process_flag(trap_exit, Trap),
            ok
    after
        3000 ->
            exit(Broker, kill),
            _ = process_flag(trap_exit, Trap),
            exit(timeout)
    end.

start_link_args(_) ->
    [init()].

init() ->
    frequency([{10, {ok, {queue_spec(), queue_spec(), 10000}}},
               {1, ignore},
               {1, bad}]).

queue_spec() ->
    {sbroker_statem_queue, resize(4, list(oneof([0, choose(1, 2)]))),
     oneof([out, out_r]), oneof([choose(0, 5), infinity]),
     oneof([drop, drop_r])}.

start_link(Init) ->
    application:set_env(sbroker, ?MODULE, Init),
    Trap = process_flag(trap_exit, true),
    case sbroker:start_link(?MODULE, []) of
        {error, Reason} = Error ->
            receive
                {'EXIT', _, Reason} ->
                    process_flag(trap_exit, Trap),
                    Error
            after
                100 ->
                    exit({timeout, Error})
            end;
        Other ->
            process_flag(trap_exit, Trap),
            Other
    end.

init([]) ->
    {ok, Result} = application:get_env(sbroker, ?MODULE),
    Result.

start_link_pre(#state{sbroker=Broker}, _) ->
    Broker =:= undefined.

start_link_next(State, Value,
                [{ok, {{_, AskDrops, AskOut, AskSize, AskDrop},
                       {_, BidDrops, BidOut, BidSize, BidDrop}, _}}]) ->
    Broker = {call, erlang, element, [2, Value]},
    State#state{sbroker=Broker, bid_out=BidOut, bid_drops=BidDrops,
                bid_state=BidDrops, bid_size=BidSize, bid_drop=BidDrop,
                ask_out=AskOut, ask_drops=AskDrops, ask_state=AskDrops,
                ask_size=AskSize, ask_drop=AskDrop};
start_link_next(State, _, [ignore]) ->
    State;
start_link_next(State, _, [bad]) ->
    State.

start_link_post(_, [{ok, _}], {ok, Broker}) when is_pid(Broker) ->
    true;
start_link_post(_, [ignore], ignore) ->
    true;
start_link_post(_, [bad], {error, {bad_return, {?MODULE, init, bad}}}) ->
    true;
start_link_post(_, _, _) ->
    false.

spawn_client_args(#state{sbroker=Broker}) ->
    [Broker, oneof([async_bid, async_ask, nb_bid, nb_ask])].

spawn_client(Broker, Fun) ->
    {ok, Pid, MRef} = proc_lib:start(?MODULE, client_init, [Broker, Fun]),
    {Pid, MRef}.

spawn_client_next(#state{asks=[], bids=[], bid_size=0} = State, _,
                  [_, async_bid]) ->
    State;
spawn_client_next(#state{asks=[]} = State, Bid, [_, async_bid]) ->
    bid_next(State,
             fun(#state{bids=NBids} = NState) ->
                     NState#state{bids=NBids++[Bid]}
             end);
spawn_client_next(#state{asks=[]} = State, _, [_, nb_bid]) ->
    State;
spawn_client_next(#state{asks=[_ | _]} = State, Bid, [_, BidFun] = Args)
  when BidFun =:= async_bid orelse BidFun =:= nb_bid ->
    ask_next(State,
             fun(#state{asks=[]} = NState) ->
                     spawn_client_next(NState, Bid, Args);
                (#state{asks=NAsks, ask_out=out, done=Done} = NState) ->
                     NState#state{asks=dropfirst(NAsks),
                                  done=Done++[Bid, hd(NAsks)]};
                (#state{asks=NAsks, ask_out=out_r, done=Done} = NState) ->
                     NState#state{asks=droplast(NAsks),
                                  done=Done++[Bid, lists:last(NAsks)]}
             end);
spawn_client_next(#state{bids=[], asks=[], ask_size=0} = State, _,
                  [_, async_ask]) ->
    State;
spawn_client_next(#state{bids=[]} = State, Ask, [_, async_ask]) ->
    ask_next(State,
             fun(#state{asks=NAsks} = NState) ->
                     NState#state{asks=NAsks++[Ask]}
             end);
spawn_client_next(#state{bids=[]} = State, _, [_, nb_ask]) ->
    State;
spawn_client_next(#state{bids=[_ | _]} = State, Ask, [_, AskFun] = Args)
  when AskFun =:= async_ask orelse AskFun =:= nb_ask ->
    bid_next(State,
             fun(#state{bids=[]} = NState) ->
                     spawn_client_next(NState, Ask, Args);
                (#state{bids=NBids, bid_out=out, done=Done} = NState) ->
                     NState#state{bids=dropfirst(NBids),
                                  done=Done++[Ask, hd(NBids)]};
                (#state{bids=NBids, bid_out=out_r, done=Done} = NState) ->
                     NState#state{bids=droplast(NBids),
                                  done=Done++[Ask, lists:last(NBids)]}
             end).

spawn_client_post(#state{asks=[], bids=[], bid_size=0}, [_, async_bid], Bid) ->
    drop_post(Bid);
spawn_client_post(#state{asks=[]} = State, [_, async_bid], Bid) ->
    bid_post(State,
             fun(#state{bids=NBids} = NState) ->
                     {true, NState#state{bids=NBids++[Bid]}}
             end);
spawn_client_post(#state{asks=[]}, [_, nb_bid], Bid) ->
    retry_post(Bid);
spawn_client_post(#state{asks=[_ | _]} = State, [_, BidFun] = Args, Bid)
  when BidFun =:= async_bid orelse BidFun =:= nb_bid ->
    ask_post(State,
             fun(#state{asks=[]} = NState) ->
                     {spawn_client_post(NState, Args, Bid),
                      spawn_client_next(NState, Bid, Args)};
                (#state{asks=NAsks, ask_out=out} = NState) ->
                     {settled_post(Bid, hd(NAsks)),
                      NState#state{asks=dropfirst(NAsks)}};
                (#state{asks=NAsks, ask_out=out_r} = NState) ->
                     {settled_post(Bid, lists:last(NAsks)),
                      NState#state{asks=droplast(NAsks)}}
             end);
spawn_client_post(#state{bids=[], asks=[], ask_size=0}, [_, async_ask], Ask) ->
    drop_post(Ask);
spawn_client_post(#state{bids=[]} = State, [_, async_ask], Ask) ->
    ask_post(State,
             fun(#state{asks=NAsks} = NState) ->
                     {true, NState#state{asks=NAsks++[Ask]}}
             end);
spawn_client_post(#state{bids=[]}, [_, nb_ask], Ask) ->
    retry_post(Ask);
spawn_client_post(#state{bids=[_ | _]} = State, [_, AskFun] = Args, Ask)
  when AskFun =:= async_ask orelse AskFun =:= nb_ask ->
    bid_post(State,
             fun(#state{bids=[]} = NState) ->
                     {spawn_client_post(NState, Args, Ask),
                      spawn_client_next(NState, Ask, Args)};
                (#state{bids=NBids, bid_out=out} = NState) ->
                     {settled_post(Ask, hd(NBids)),
                      NState#state{bids=dropfirst(NBids)}};
                (#state{bids=NBids, bid_out=out_r} = NState) ->
                     {settled_post(Ask, lists:last(NBids)),
                      NState#state{bids=droplast(NBids)}}
             end).

timeout_args(#state{sbroker=Broker}) ->
    [Broker].

timeout_next(#state{asks=[]} = State, _, _) ->
    bid_next(State, fun(NState) -> NState end);
timeout_next(#state{bids=[]} = State, _, _) ->
    ask_next(State, fun(NState) -> NState end).

timeout_post(#state{asks=[]} = State, _, _) ->
    bid_post(State, fun(NState) -> {true, NState} end);
timeout_post(#state{bids=[]} = State, _, _) ->
    ask_post(State, fun(NState) -> {true, NState} end).

cancel_args(#state{sbroker=Broker, asks=Asks, bids=Bids, cancels=Cancels,
                   done=Done}) ->
    case Asks ++ Bids ++ Cancels ++ Done of
        [] ->
            [Broker, undefined];
        Clients ->
            [Broker, frequency([{9, elements(Clients)},
                                {1, undefined}])]
    end.

cancel(Broker, undefined) ->
    sbroker:cancel(Broker, make_ref(), ?TIMEOUT);
cancel(_, Client) ->
    client_call(Client, cancel).

cancel_next(#state{asks=[]} = State, _, [_, Client]) ->
    bid_next(State,
             fun(#state{bids=NBids, cancels=Cancels} = NState) ->
                     case lists:member(Client, NBids) of
                         true ->
                             NState#state{bids=NBids--[Client],
                                          cancels=Cancels++[Client]};
                         false ->
                             NState
                     end
             end);
cancel_next(#state{bids=[]} = State, _, [_, Client]) ->
    ask_next(State,
             fun(#state{asks=NAsks, cancels=Cancels} = NState) ->
                     case lists:member(Client, NAsks) of
                         true ->
                             NState#state{asks=NAsks--[Client],
                                          cancels=Cancels++[Client]};
                         false ->
                             NState
                     end
             end).

cancel_post(#state{asks=[]} = State, [_, Client], Result) ->
    bid_post(State,
             fun(#state{bids=Bids} = NState) ->
                     case lists:member(Client, Bids) of
                         true when Result =:= 1 ->
                             {true, NState#state{bids=Bids--[Client]}};
                         true ->
                             ct:pal("Cancel: ~p", [Result]),
                             {false, NState#state{bids=Bids--[Client]}};
                         false when Result =:= false ->
                             {true, NState};
                         false ->
                             ct:pal("Cancel: ~p", [Result]),
                             {false, NState}
                     end
             end);
cancel_post(#state{bids=[]} = State, [_, Client], Result) ->
    ask_post(State,
             fun(#state{asks=Asks} = NState) ->
                     case lists:member(Client, Asks) of
                         true when Result =:= 1 ->
                             {true, NState#state{asks=Asks--[Client]}};
                         true ->
                             ct:pal("Cancel: ~p", [Result]),
                             {false, NState#state{asks=Asks--[Client]}};
                         false when Result =:= false ->
                             {true, NState};
                         false ->
                             ct:pal("Cancel: ~p", [Result]),
                             {false, NState}
                     end
             end).

change_config_args(#state{sbroker=Broker}) ->
    [Broker, init()].

change_config(Broker, Init) ->
    application:set_env(sbroker, ?MODULE, Init),
    sbroker:change_config(Broker, 100).

change_config_next(State, _, [_, ignore]) ->
    State;
change_config_next(State, _, [_, bad]) ->
    State;
change_config_next(State, _,
                   [_, {ok, {AskQueueSpec, BidQueueSpec, _}}]) ->
    NState = ask_change(AskQueueSpec, State),
    bid_change(BidQueueSpec, NState).

%% If Mod Args the same the state of the squeue does not change.
ask_change({_, AskDrops, AskOut, AskSize, AskDrop},
           #state{ask_drops=AskDrops} = State) ->
    State#state{ask_out=AskOut, ask_size=AskSize, ask_drop=AskDrop};
ask_change({_, AskDrops, AskOut, AskSize, AskDrop}, State) ->
    State#state{ask_out=AskOut, ask_drops=AskDrops, ask_state=AskDrops,
                ask_size=AskSize, ask_drop=AskDrop}.

bid_change({_, BidDrops, BidOut, BidSize, BidDrop},
           #state{bid_drops=BidDrops} = State) ->
    State#state{bid_out=BidOut, bid_size=BidSize, bid_drop=BidDrop};
bid_change({_, BidDrops, BidOut, BidSize, BidDrop}, State) ->
    State#state{bid_out=BidOut, bid_drops=BidDrops, bid_state=BidDrops,
                bid_size=BidSize, bid_drop=BidDrop}.

change_config_post(_, [_, ignore], ok) ->
    true;
change_config_post(_, [_, bad],
                   {error, {'EXIT', {bad_return, {?MODULE, init, bad}}}}) ->
    true;
change_config_post(_, [_, {ok, _}], ok) ->
    true;
change_config_post(_, _, _) ->
    false.

shutdown_client_args(#state{sbroker=Broker, asks=Asks, bids=Bids, cancels=Cancels,
                   done=Done}) ->
    case Asks ++ Bids ++ Cancels ++ Done of
        [] ->
            [Broker, undefined];
        Clients ->
            [Broker, frequency([{9, elements(Clients)},
                                {1, undefined}])]
    end.

shutdown_client(Broker, undefined) ->
    _ = Broker ! {'DOWN', make_ref(), process, self(), shutdown},
    ok;
shutdown_client(Broker, Client) ->
    Pid = client_pid(Client),
    MRef = monitor(process, Pid),
    exit(Pid, shutdown),
    receive
        {'DOWN', MRef, _, _, shutdown} ->
            %% Sync with broker so that the next command does not arrive before
            %% the monitor message if broker is behind.
            _ = sys:get_status(Broker),
            ok;
        {'DOWN', MRef, _, _, Reason} ->
            exit(Reason)
    after
        100 ->
            exit(timeout)
    end.

shutdown_client_next(#state{asks=[]} = State, _, [_, undefined]) ->
    bid_next(State, fun(NState) -> NState end);
shutdown_client_next(#state{bids=[]} = State, _, [_, undefined]) ->
    ask_next(State, fun(NState) -> NState end);
shutdown_client_next(#state{bids=Bids, asks=Asks, cancels=Cancels,
                            done=Done} = State, _, [_, Client]) ->
    case {lists:member(Client, Bids), lists:member(Client, Asks)} of
        {true, false} ->
            #state{bids=NBids, done=NDone} = NState = bid_aqm_next(State),
            NState2 = NState#state{bids=NBids--[Client], done=NDone--[Client]},
            bid_pqm_next(NState2);
        {false, true} ->
            #state{asks=NAsks, done=NDone} = NState = ask_aqm_next(State),
            NState2 = NState#state{asks=NAsks--[Client], done=NDone--[Client]},
            ask_pqm_next(NState2);
        {false, false} ->
            State#state{cancels=Cancels--[Client], done=Done--[Client]}
    end.

shutdown_client_post(#state{asks=[]} = State, [_, undefined], _) ->
    bid_post(State, fun(NState) -> {true, NState} end);
shutdown_client_post(#state{bids=[]} = State, [_, undefined], _) ->
    ask_post(State, fun(NState) -> {true, NState} end);
shutdown_client_post(#state{asks=Asks, bids=Bids} = State, [_, Client], _) ->
    case {lists:member(Client, Asks), lists:member(Client, Bids)} of
        {true, false} ->
            {Drops, #state{bids=NBids} = NState} = bid_aqm(State),
            NState2 = NState#state{bids=NBids--[Client]},
            drops_post(Drops--[Client]) andalso bid_pqm_post(NState2);
        {false, true} ->
            {Drops, #state{asks=NAsks} = NState} = ask_aqm(State),
            NState2 = NState#state{asks=NAsks--[Client]},
            drops_post(Drops--[Client]) andalso ask_pqm_post(NState2);
        {false, false} ->
            true
    end.

%% Helpers

bid_next(State, Fun) ->
    NState = bid_aqm_next(State),
    NState2 = Fun(NState),
    bid_pqm_next(NState2).

bid_post(State, Fun) ->
    {Drops, NState} = bid_aqm(State),
    {Result, NState2} = Fun(NState),
    Result andalso drops_post(Drops) andalso bid_pqm_post(NState2).

ask_next(State, Fun) ->
    NState = ask_aqm_next(State),
    NState2 = Fun(NState),
    ask_pqm_next(NState2).

ask_post(State, Fun) ->
    {Drops, NState} = ask_aqm(State),
    {Result, NState2} = Fun(NState),
    Result andalso drops_post(Drops) andalso ask_pqm_post(NState2).

droplast([]) ->
    [];
droplast(List) ->
    [_ | Rest] = lists:reverse(List),
    lists:reverse(Rest).

dropfirst([]) ->
    [];
dropfirst([_ | Rest]) ->
    Rest.

bid_aqm_next(State) ->
    {_, NState} = bid_aqm(State),
    NState.

bid_aqm(#state{bids=[]} = State) ->
    {[], State};
bid_aqm(#state{bid_state=[], bid_drops=[]} = State) ->
    {[], State};
bid_aqm(#state{bid_state=[], bid_drops=BidDrops} = State) ->
    bid_aqm(State#state{bid_state=BidDrops});
bid_aqm(#state{bids=Bids, bid_state=[BidDrop | NBidState],
                done=Done} = State) ->
    BidDrop2 = min(length(Bids), BidDrop),
    {Drops, NBids} = lists:split(BidDrop2, Bids),
    {Drops, State#state{bids=NBids, bid_state=NBidState, done=Done++Drops}}.

ask_aqm_next(State) ->
    {_, NState} = ask_aqm(State),
    NState.

ask_aqm(#state{asks=[]} = State) ->
    {[], State};
ask_aqm(#state{ask_state=[], ask_drops=[]} = State) ->
    {[], State};
ask_aqm(#state{ask_state=[], ask_drops=AskDrops} = State) ->
    ask_aqm(State#state{ask_state=AskDrops});
ask_aqm(#state{asks=Asks, ask_state=[AskDrop | NAskState],
                done=Done} = State) ->
    AskDrop2 = min(length(Asks), AskDrop),
    {Drops, NAsks} = lists:split(AskDrop2, Asks),
    {Drops, State#state{asks=NAsks, ask_state=NAskState, done=Done++Drops}}.

drops_post([]) ->
    true;
drops_post([Client | Drops]) ->
    drop_post(Client) andalso drops_post(Drops).

drop_post(Client) ->
    case result(Client) of
        {drop, _} ->
            true;
        Other ->
            ct:log("~p Drop: ~p", [Client, Other]),
            false
    end.

settled_post(Client1, Client2) ->
    Pid1 = client_pid(Client1),
    Pid2 = client_pid(Client2),
    case {result(Client1), result(Client2)} of
        {{go, Ref, Pid2, RelSojournTime1, SojournTime1},
         {go, Ref, Pid1, RelSojournTime2, SojournTime2}} ->
            is_integer(SojournTime1) andalso SojournTime1 >= 0 andalso
            is_integer(SojournTime2) andalso SojournTime2 >= 0 andalso
            is_integer(RelSojournTime1) andalso RelSojournTime1 =< 0 andalso
            -RelSojournTime1 =:= RelSojournTime2;
        Result ->
            ct:log("Result: ~p", [Result]),
            false
    end.

retry_post(Client) ->
    case result(Client) of
        {retry, SojournTime} ->
            is_integer(SojournTime) andalso SojournTime >= 0;
        Result ->
            ct:log("Result ~p", [Result]),
            false
    end.

result(Client) ->
    client_call(Client, result).

bid_pqm_next(#state{bids=Bids, bid_size=BidSize, bid_drop=BidDrop} = State)
  when length(Bids) > BidSize ->
    {Drops, #state{bids=NBids} = NState} = bid_aqm(State),
    case Drops of
        [] when BidDrop =:= drop ->
            bid_pqm_next(NState#state{bids=dropfirst(NBids)});
        [] when BidDrop =:= drop_r ->
            bid_pqm_next(NState#state{bids=droplast(NBids)});
        _ ->
            bid_pqm_next(NState)
    end;
bid_pqm_next(State) ->
    State.

bid_pqm_post(#state{bids=Bids, bid_size=BidSize, bid_drop=BidDrop} = State)
  when length(Bids) > BidSize ->
    {Drops, #state{bids=NBids} = NState} = bid_aqm(State),
    case Drops of
        [] when BidDrop =:= drop ->
            drops_post(Drops) andalso
            bid_pqm_post(NState#state{bids=dropfirst(NBids)});
        [] when BidDrop =:= drop_r ->
            drops_post(Drops) andalso
            bid_pqm_post(NState#state{bids=droplast(NBids)});
        _ ->
            drops_post(Drops) andalso bid_pqm_post(NState)
    end;
bid_pqm_post(_) ->
    true.

ask_pqm_next(#state{asks=Asks, ask_size=AskSize, ask_drop=AskDrop} = State)
  when length(Asks) > AskSize ->
    {Drops, #state{asks=NAsks} = NState} = ask_aqm(State),
    case Drops of
        [] when AskDrop =:= drop ->
            ask_pqm_next(NState#state{asks=dropfirst(NAsks)});
        [] when AskDrop =:= drop_r ->
            ask_pqm_next(NState#state{asks=droplast(NAsks)});
        _ ->
            ask_pqm_next(NState)
    end;
ask_pqm_next(State) ->
    State.

ask_pqm_post(#state{asks=Asks, ask_size=AskSize, ask_drop=AskDrop} = State)
  when length(Asks) > AskSize ->
    {Drops, #state{asks=NAsks} = NState} = ask_aqm(State),
    case Drops of
        [] when AskDrop =:= drop ->
            drops_post(Drops) andalso
            ask_pqm_post(NState#state{asks=dropfirst(NAsks)});
        [] when AskDrop =:= drop_r ->
            drops_post(Drops) andalso
            ask_pqm_post(NState#state{asks=droplast(NAsks)});
        _ ->
            drops_post(Drops) andalso ask_pqm_post(NState)
    end;
ask_pqm_post(_) ->
    true.

%% client

client_pid({Pid, _}) ->
    Pid.

client_call({Pid, MRef}, Call) ->
    try gen:call(Pid, MRef, Call, 100) of
        {ok, Response} ->
            Response
    catch
        exit:Reason ->
            {exit, Reason}
    end.

client_init(Broker, async_bid) ->
    MRef = monitor(process, Broker),
    ARef = make_ref(),
    {await, ARef, Broker} = sbroker:async_ask_r(Broker, ARef),
    client_init(MRef, Broker, ARef, queued);
client_init(Broker, nb_bid) ->
    MRef = monitor(process, Broker),
    State = sbroker:nb_ask_r(Broker),
    client_init(MRef, Broker, undefined, State);
client_init(Broker, async_ask) ->
    MRef = monitor(process, Broker),
    ARef = make_ref(),
    {await, ARef, Broker} = sbroker:async_ask(Broker, ARef),
    client_init(MRef, Broker, ARef, queued);
client_init(Broker, nb_ask) ->
    MRef = monitor(process, Broker),
    State = sbroker:nb_ask(Broker),
    client_init(MRef, Broker, undefined, State).

client_init(MRef, Broker, ARef, State) ->
    proc_lib:init_ack({ok, self(), MRef}),
    client_loop(MRef, Broker, ARef, State, []).

client_loop(MRef, Broker, ARef, State, Froms) ->
    receive
        {'DOWN', MRef, _, _, _} ->
            exit(normal);
        {ARef, Result} when State =:= queued ->
            _ = [gen:reply(From, Result) || From <- Froms],
            client_loop(MRef, Broker, ARef, Result, []);
        {ARef, Result} when State =/= queued ->
            exit(Broker, {double_result, {self(), MRef}, State, Result}),
            exit(normal);
        {MRef, From, cancel} ->
            case sbroker:cancel(Broker, ARef, ?TIMEOUT) of
                N when is_integer(N) ->
                    gen:reply(From, N),
                    client_loop(MRef, Broker, ARef, cancelled, Froms);
                false ->
                    gen:reply(From, false),
                    client_loop(MRef, Broker, ARef, State, Froms)
            end;
        {MRef, From, result} when State =:= queued ->
            client_loop(MRef, Broker, ARef, State, [From | Froms]);
        {MRef, From, result} ->
            gen:reply(From, State),
            client_loop(MRef, Broker, ARef, State, Froms)
    end.
