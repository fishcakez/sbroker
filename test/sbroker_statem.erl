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
-export([len/2]).
-export([change_config/2]).
-export([shutdown_client/2]).
-export([replace_state/2]).
-export([change_code/3]).

-export([client_init/2]).

-record(state, {sbroker, asks=[], ask_mod, ask_out, ask_drops, ask_state=[],
                bids=[], bid_mod, bid_out, bid_drops, bid_state=[], cancels=[],
                done=[], sys=running, change}).

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
command(#state{sys=running} = State) ->
    frequency([{10, {call, ?MODULE, spawn_client,
                     spawn_client_args(State)}},
               {4, {call, sbroker, timeout, timeout_args(State)}},
               {3, {call, ?MODULE, cancel, cancel_args(State)}},
               {2, {call, ?MODULE, len, len_args(State)}},
               {2, {call, ?MODULE, change_config,
                    change_config_args(State)}},
               {2, {call, ?MODULE, shutdown_client,
                    shutdown_client_args(State)}},
               {1, {call, sys, get_status, get_status_args(State)}},
               {1, {call, sys, get_state, get_state_args(State)}},
               {1, {call, ?MODULE, replace_state, replace_state_args(State)}},
               {1, {call, sys, suspend, suspend_args(State)}}]);
command(#state{sys=suspended} = State) ->
    frequency([{5, {call, sys, resume, resume_args(State)}},
               {2, {call, ?MODULE, change_code, change_code_args(State)}},
               {1, {call, sys, get_status, get_status_args(State)}},
               {1, {call, sys, get_state, get_state_args(State)}},
               {1, {call, ?MODULE, replace_state, replace_state_args(State)}}]).

precondition(State, {call, _, start_link, Args}) ->
    start_link_pre(State, Args);
precondition(#state{sbroker=undefined}, _) ->
    false;
precondition(State, {call, _, get_status, Args}) ->
    get_status_pre(State, Args);
precondition(State, {call, _, get_state, Args}) ->
    get_state_pre(State, Args);
precondition(State, {call, _, replace_state, Args}) ->
    replace_state_pre(State, Args);
precondition(State, {call, _, suspend, Args}) ->
    suspend_pre(State, Args);
precondition(State, {call, _, change_code, Args}) ->
    change_code_pre(State, Args);
precondition(State, {call, _, resume, Args}) ->
    resume_pre(State, Args);
precondition(#state{sys=suspended}, _) ->
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
next_state(State, Value, {call, _, len, Args}) ->
    len_next(State, Value, Args);
next_state(State, Value, {call, _, change_config, Args}) ->
    change_config_next(State, Value, Args);
next_state(State, Value, {call, _, shutdown_client, Args}) ->
    shutdown_client_next(State, Value, Args);
next_state(State, Value, {call, _, get_status, Args}) ->
    get_status_next(State, Value, Args);
next_state(State, Value, {call, _, get_state, Args}) ->
    get_state_next(State, Value, Args);
next_state(State, Value, {call, _, replace_state, Args}) ->
    replace_state_next(State, Value, Args);
next_state(State, Value, {call, _, suspend, Args}) ->
    suspend_next(State, Value, Args);
next_state(State, Value, {call, _, change_code, Args}) ->
    change_code_next(State, Value, Args);
next_state(State, Value, {call, _, resume, Args}) ->
    resume_next(State, Value, Args);
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
postcondition(State, {call, _, len, Args}, Result) ->
    len_post(State, Args, Result);
postcondition(State, {call, _, change_config, Args}, Result) ->
    change_config_post(State, Args, Result);
postcondition(State, {call, _, shutdown_client, Args}, Result) ->
    shutdown_client_post(State, Args, Result);
postcondition(State, {call, _, get_status, Args}, Result) ->
    get_status_post(State, Args, Result);
postcondition(State, {call, _, get_state, Args}, Result) ->
    get_state_post(State, Args, Result);
postcondition(State, {call, _, replace_state, Args}, Result) ->
    replace_state_post(State, Args, Result);
postcondition(State, {call, _, suspend, Args}, Result) ->
    suspend_post(State, Args, Result);
postcondition(State, {call, _, change_code, Args}, Result) ->
    change_code_post(State, Args, Result);
postcondition(State, {call, _, resume, Args}, Result) ->
    resume_post(State, Args, Result);
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
    frequency([{30, {ok, {queue_spec(), queue_spec(), 10000}}},
               {1, ignore},
               {1, bad}]).

queue_spec() ->
    {oneof([sbroker_statem_queue, sbroker_statem2_queue]),
     {oneof([out, out_r]), resize(4, list(oneof([0, choose(1, 2)])))}}.

start_link(Init) ->
    application:set_env(sbroker, ?MODULE, Init),
    Trap = process_flag(trap_exit, true),
    case sbroker:start_link(?MODULE, [], []) of
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
                [{ok, {{AskMod, {AskOut, AskDrops}},
                       {BidMod, {BidOut, BidDrops}}, _}}]) ->
    Broker = {call, erlang, element, [2, Value]},
    State#state{sbroker=Broker, bid_mod=BidMod, bid_out=BidOut,
                bid_drops=BidDrops, bid_state=BidDrops, ask_mod=AskMod,
                ask_out=AskOut, ask_drops=AskDrops, ask_state=AskDrops};
start_link_next(State, _, [ignore]) ->
    State;
start_link_next(State, _, [bad]) ->
    State.

start_link_post(_, [{ok, _}], {ok, Broker}) when is_pid(Broker) ->
    true;
start_link_post(_, [ignore], ignore) ->
    true;
start_link_post(_, [bad], {error, {bad_return_value, bad}}) ->
    true;
start_link_post(_, _, _) ->
    false.

spawn_client_args(#state{sbroker=Broker}) ->
    [Broker, oneof([async_bid, async_ask, nb_bid, nb_ask])].

spawn_client(Broker, Fun) ->
    {ok, Pid, MRef} = proc_lib:start(?MODULE, client_init, [Broker, Fun]),
    {Pid, MRef}.

spawn_client_next(#state{asks=[]} = State, Bid, [_, async_bid]) ->
    bid_next(State,
             fun(#state{bids=NBids} = NState) ->
                     NState#state{bids=NBids++[Bid]}
             end);
spawn_client_next(#state{asks=[]} = State, _, [_, nb_bid]) ->
    timeout_next(State);
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
spawn_client_next(#state{bids=[]} = State, Ask, [_, async_ask]) ->
    ask_next(State,
             fun(#state{asks=NAsks} = NState) ->
                     NState#state{asks=NAsks++[Ask]}
             end);
spawn_client_next(#state{bids=[]} = State, _, [_, nb_ask]) ->
    timeout_next(State);
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

spawn_client_post(#state{asks=[]} = State, [_, async_bid], _) ->
    bid_aqm_post(State);
spawn_client_post(#state{asks=[]} = State, [_, nb_bid], Bid) ->
    retry_post(Bid) andalso timeout_post(State);
spawn_client_post(#state{asks=[_ | _]} = State, [_, BidFun] = Args, Bid)
  when BidFun =:= async_bid orelse BidFun =:= nb_bid ->
    ask_post(State,
             fun(#state{asks=[]} = NState) ->
                     spawn_client_post(NState, Args, Bid);
                (#state{asks=NAsks, ask_out=out}) ->
                     settled_post(Bid, hd(NAsks));
                (#state{asks=NAsks, ask_out=out_r}) ->
                     settled_post(Bid, lists:last(NAsks))
             end);
spawn_client_post(#state{bids=[]} = State, [_, async_ask], _) ->
    ask_aqm_post(State);
spawn_client_post(#state{bids=[]} = State, [_, nb_ask], Ask) ->
    retry_post(Ask) andalso timeout_post(State);
spawn_client_post(#state{bids=[_ | _]} = State, [_, AskFun] = Args, Ask)
  when AskFun =:= async_ask orelse AskFun =:= nb_ask ->
    bid_post(State,
             fun(#state{bids=[]} = NState) ->
                     spawn_client_post(NState, Args, Ask);
                (#state{bids=NBids, bid_out=out}) ->
                     settled_post(Ask, hd(NBids));
                (#state{bids=NBids, bid_out=out_r}) ->
                     settled_post(Ask, lists:last(NBids))
             end).

timeout_args(#state{sbroker=Broker}) ->
    [Broker].

timeout_next(State, _, _) ->
    timeout_next(State).

timeout_next(#state{asks=[]} = State) ->
    bid_next(State, fun(NState) -> NState end);
timeout_next(#state{bids=[]} = State) ->
    ask_next(State, fun(NState) -> NState end).

timeout_post(State, _, _) ->
    timeout_post(State).

timeout_post(#state{asks=[]} = State) ->
    bid_aqm_post(State);
timeout_post(#state{bids=[]} = State) ->
    ask_aqm_post(State).

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
             fun(#state{bids=Bids}) ->
                     case lists:member(Client, Bids) of
                         true when Result =:= 1 ->
                             true;
                         true ->
                             ct:pal("Cancel: ~p", [Result]),
                             false;
                         false when Result =:= false ->
                             true;
                         false ->
                             ct:pal("Cancel: ~p", [Result]),
                             false
                     end
             end);
cancel_post(#state{bids=[]} = State, [_, Client], Result) ->
    ask_post(State,
             fun(#state{asks=Asks}) ->
                     case lists:member(Client, Asks) of
                         true when Result =:= 1 ->
                             true;
                         true ->
                             ct:pal("Cancel: ~p", [Result]),
                             false;
                         false when Result =:= false ->
                             true;
                         false ->
                             ct:pal("Cancel: ~p", [Result]),
                             false
                     end
             end).

len_args(#state{sbroker=Broker}) ->
    [Broker, oneof([ask, bid])].

len(Broker, ask) ->
    sbroker:len(Broker, ?TIMEOUT);
len(Broker, bid) ->
    sbroker:len_r(Broker, ?TIMEOUT).

len_next(State, _, _) ->
    timeout_next(State).

len_post(#state{asks=Asks} = State, [_, ask], Len) ->
    length(Asks) =:= Len andalso timeout_post(State);
len_post(#state{bids=Bids} = State, [_, bid], Len) ->
    length(Bids) =:= Len andalso timeout_post(State).

change_config_args(#state{sbroker=Broker}) ->
    [Broker, init()].

change_config(Broker, Init) ->
    application:set_env(sbroker, ?MODULE, Init),
    sbroker:change_config(Broker, 100).

change_config_next(State, _, [_, ignore]) ->
    timeout_next(State);
change_config_next(State, _, [_, bad]) ->
    timeout_next(State);
change_config_next(State, _,
                   [_, {ok, {AskQueueSpec, BidQueueSpec, _}}]) ->
    NState = ask_change_next(AskQueueSpec, State),
    bid_change_next(BidQueueSpec, NState).

%% If module and args the same state not changed by backends.
ask_change_next({AskMod, {AskOut, AskDrops}},
           #state{ask_mod=AskMod, ask_drops=AskDrops} = State) ->
    ask_aqm_next(State#state{ask_out=AskOut});
%% If module and/or args different reset the backend state.
ask_change_next({AskMod, {AskOut, AskDrops}}, #state{ask_mod=AskMod} = State) ->
    ask_aqm_next(State#state{ask_mod=AskMod, ask_out=AskOut, ask_drops=AskDrops,
                             ask_state=AskDrops});
ask_change_next({AskMod, {AskOut, AskDrops}}, #state{asks=Asks} = State) ->
    NState = State#state{ask_mod=AskMod, asks=[], ask_out=AskOut,
                         ask_drops=AskDrops, ask_state=AskDrops},
    In = fun(Client, StateAcc) ->
                 ask_next(StateAcc,
                          fun(#state{asks=NAsks} = NStateAcc) ->
                                  NStateAcc#state{asks=NAsks++[Client]}
                          end)
         end,
    lists:foldl(In, NState, Asks).

bid_change_next({BidMod, {BidOut, BidDrops}},
           #state{bid_mod=BidMod, bid_drops=BidDrops} = State) ->
    bid_aqm_next(State#state{bid_out=BidOut});
bid_change_next({BidMod, {BidOut, BidDrops}}, #state{bid_mod=BidMod} = State) ->
    bid_aqm_next(State#state{bid_mod=BidMod, bid_out=BidOut, bid_drops=BidDrops,
                             bid_state=BidDrops});
bid_change_next({BidMod, {BidOut, BidDrops}}, #state{bids=Bids} = State) ->
    NState = State#state{bid_mod=BidMod, bids=[], bid_out=BidOut,
                         bid_drops=BidDrops, bid_state=BidDrops},
    In = fun(Client, StateAcc) ->
                 bid_next(StateAcc,
                          fun(#state{bids=NBids} = NStateAcc) ->
                                  NStateAcc#state{bids=NBids++[Client]}
                          end)
         end,
    lists:foldl(In, NState, Bids).

change_config_post(State, [_, ignore], ok) ->
    timeout_post(State);
change_config_post(State, [_, bad], {error, {bad_return_value, bad}}) ->
    timeout_post(State);
change_config_post(State, [_, {ok, {AskQueueSpec, BidQueueSpec, _}}], ok) ->
    ask_change_post(AskQueueSpec, State) andalso
    bid_change_post(BidQueueSpec, State);
change_config_post(_, _, _) ->
    false.

ask_change_post({AskMod, {AskOut, AskDrops}},
           #state{ask_mod=AskMod, ask_drops=AskDrops} = State) ->
    ask_aqm_post(State#state{ask_out=AskOut});
%% If module and/or args different reset the backend state.
ask_change_post({AskMod, {AskOut, AskDrops}}, #state{ask_mod=AskMod} = State) ->
    ask_aqm_post(State#state{ask_mod=AskMod, ask_out=AskOut, ask_drops=AskDrops,
                             ask_state=AskDrops});
ask_change_post({AskMod, {AskOut, AskDrops}}, #state{asks=Asks} = State) ->
    NState = State#state{ask_mod=AskMod, asks=[], ask_out=AskOut,
                         ask_drops=AskDrops, ask_state=AskDrops},
    In = fun(Client, {true, StateAcc}) ->
                 {Drops, #state{asks=NAsks} = NStateAcc} = ask_aqm(StateAcc),
                 {drops_post(Drops), NStateAcc#state{asks=NAsks++[Client]}};
            (_, {false, _} = Acc) ->
                 Acc
         end,
    {Result, _} = lists:foldl(In, {true, NState}, Asks),
    Result.

bid_change_post({BidMod, {BidOut, BidDrops}},
                #state{bid_mod=BidMod, bid_drops=BidDrops} = State) ->
    bid_aqm_post(State#state{bid_out=BidOut});
bid_change_post({BidMod, {BidOut, BidDrops}}, #state{bid_mod=BidMod} = State) ->
    bid_aqm_post(State#state{bid_mod=BidMod, bid_out=BidOut, bid_drops=BidDrops,
                             bid_state=BidDrops});
bid_change_post({BidMod, {BidOut, BidDrops}}, #state{bids=Bids} = State) ->
    NState = State#state{bid_mod=BidMod, bids=[], bid_out=BidOut,
                         bid_drops=BidDrops, bid_state=BidDrops},
    In = fun(Client, {true, StateAcc}) ->
                 {Drops, #state{bids=NBids} = NStateAcc} = ask_aqm(StateAcc),
                 {drops_post(Drops), NStateAcc#state{bids=NBids++[Client]}};
            (_, {false, _} = Acc) ->
                 Acc
         end,
    {Result, _} = lists:foldl(In, {true, NState}, Bids),
    Result.

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
shutdown_client(_, Client) ->
    Pid = client_pid(Client),
    MRef = monitor(process, Pid),
    exit(Pid, shutdown),
    receive
        {'DOWN', MRef, _, _, shutdown} ->
            %% Sync with broker so that the next command does not arrive before
            %% the monitor message if broker is behind.
            timer:sleep(50),
            ok;
        {'DOWN', MRef, _, _, Reason} ->
            exit(Reason)
    after
        100 ->
            exit(timeout)
    end.

shutdown_client_next(#state{asks=[]} = State, _, [_, undefined]) ->
    bid_aqm_next(State);
shutdown_client_next(#state{bids=[]} = State, _, [_, undefined]) ->
    ask_aqm_next(State);
shutdown_client_next(#state{bids=Bids, asks=Asks, cancels=Cancels,
                            done=Done} = State, _, [_, Client]) ->
    case lists:member(Client, Bids) orelse lists:member(Client, Asks) of
        true ->
            #state{bids=NBids} = NState = bid_aqm_next(State),
            NState2 = NState#state{bids=NBids--[Client]},
            #state{asks=NAsks, done=NDone} = NState3 = ask_aqm_next(NState2),
            NState3#state{asks=NAsks--[Client], done=NDone--[Client]};
        false ->
            State#state{cancels=Cancels--[Client], done=Done--[Client]}
    end.

shutdown_client_post(#state{asks=[]} = State, [_, undefined], _) ->
    bid_aqm_post(State);
shutdown_client_post(#state{bids=[]} = State, [_, undefined], _) ->
    ask_aqm_post(State);
shutdown_client_post(#state{asks=Asks, bids=Bids} = State, [_, Client], _) ->
    case lists:member(Client, Asks) orelse lists:member(Client, Bids) of
        true ->
            {Drops1, NState} = bid_aqm(State),
            {Drops2, _} = ask_aqm(NState),
            drops_post(Drops1--[Client]) andalso drops_post(Drops2--[Client]);
        false ->
            true
    end.

get_status_args(#state{sbroker=Broker}) ->
    [Broker, ?TIMEOUT].

get_status_pre(_, _) ->
    true.

get_status_next(State, _, _) ->
    sys_next(State).

get_status_post(#state{sbroker=Broker, sys=Sys} = State, _,
                {status, Pid, {module, Mod},
                 [_, SysState, Parent, _, Status]}) ->
    Pid =:= Broker andalso Mod =:= sbroker andalso SysState =:= Sys andalso
    Parent =:= self() andalso status_post(State, Status) andalso
    sys_post(State);
get_status_post(_, _, Result) ->
    ct:pal("Full status: ~p", [Result]),
    false.

status_post(#state{asks=Asks, bids=Bids, sys=Sys} = State,
            [{header, "Status for sbroker <" ++_ },
             {data, [{"Status", SysState},
                     {"Parent", Self},
                     {"Active queue", Active},
                     {"Time", {TimeMod, native, Time}}]},
             {items, {"Installed queues", Queues}}]) ->
    SysState =:= Sys andalso Self =:= self() andalso
    ((length(Asks) > 0 andalso Active =:= ask) orelse
     (length(Bids) > 0 andalso Active =:= ask_r) orelse
     (Asks =:= [] andalso Bids =:= [])) andalso
    ((erlang:function_exported(erlang, monotonic_time, 1) andalso
      TimeMod =:= erlang) orelse TimeMod =:= sbroker_legacy) andalso
    is_integer(Time) andalso
    get_state_post(State, Queues);
status_post(_, Status) ->
    ct:pal("Status: ~p", [Status]),
    false.

get_state_args(#state{sbroker=Broker}) ->
    [Broker, ?TIMEOUT].

get_state_pre(_, _) ->
    erlang:function_exported(gen_server, system_get_state, 1).

get_state_next(State, _, _) ->
    sys_next(State).

get_state_post(State, _, Result) ->
    get_state_post(State, Result) andalso sys_post(State).

get_state_post(State, Queues) ->
    get_ask_post(State, Queues) andalso get_bid_post(State, Queues).

get_ask_post(#state{ask_mod=AskMod, asks=Asks}, Queues) ->
    case lists:keyfind(ask, 2, Queues) of
        {AskMod, ask, AskState} ->
            ClientPids = [client_pid(Client) || Client <- Asks],
            Pids = [Pid || {_, {Pid,_}, _} <- AskMod:to_list(AskState)],
            Pids =:= ClientPids;
        Ask ->
            ct:pal("Ask queue: ~p", [Ask]),
            false
    end.

get_bid_post(#state{bid_mod=BidMod, bids=Bids}, Queues) ->
    case lists:keyfind(ask_r, 2, Queues) of
        {BidMod, ask_r, BidState} ->
            ClientPids = [client_pid(Client) || Client <- Bids],
            Pids = [Pid || {_, {Pid,_}, _} <- BidMod:to_list(BidState)],
            Pids =:= ClientPids;
        Bid ->
            ct:pal("Bid queue: ~p", [Bid]),
            false
    end.

replace_state_args(#state{sbroker=Broker}) ->
    [Broker, ?TIMEOUT].

replace_state(Broker, Timeout) ->
    sys:replace_state(Broker, fun(Q) -> Q end, Timeout).

replace_state_pre(_, _) ->
    erlang:function_exported(gen_server, system_replace_state, 2).

replace_state_next(State, _, _) ->
    sys_next(State).

replace_state_post(State, _, Result) ->
    get_state_post(State, Result) andalso sys_post(State).

suspend_args(#state{sbroker=Broker}) ->
    [Broker, ?TIMEOUT].

suspend_pre(#state{sys=SysState}, _) ->
    SysState =:= running.

suspend_next(State, _, _) ->
    State#state{sys=suspended}.

suspend_post(_, _, _) ->
    true.

change_code_args(#state{sbroker=Broker}) ->
    [Broker, init(), ?TIMEOUT].

change_code(Broker, Init, Timeout) ->
    application:set_env(sbroker, ?MODULE, Init),
    sys:change_code(Broker, undefined, undefined, undefined, Timeout).

change_code_pre(#state{sys=SysState}, _) ->
    SysState =:= suspended.

change_code_next(State, _, [_, {ok, _} = Init, _]) ->
    State#state{change=Init};
change_code_next(State, _, _) ->
    State.

change_code_post(_, [_, {ok, _}, _], ok) ->
    true;
change_code_post(_, [_, ignore, _], ok) ->
    true;
change_code_post(_, [_, bad, _], {error, {bad_return_value, bad}}) ->
    true;
change_code_post(_, _, _) ->
    false.

resume_args(#state{sbroker=Broker}) ->
    [Broker, ?TIMEOUT].

resume_pre(#state{sys=SysState}, _) ->
    SysState =:= suspended.

resume_next(#state{change={ok, {AskQueueSpec, BidQueueSpec, _}}} = State, _,
            _) ->
    NState = State#state{sys=running, change=undefined},
    NState2 = ask_change_next(AskQueueSpec, NState),
    bid_change_next(BidQueueSpec, NState2);
resume_next(State, _, _) ->
    timeout_next(State#state{sys=running}).

resume_post(#state{change={ok, {AskQueueSpec, BidQueueSpec, _}}} = State, _,
            _) ->
    ask_change_post(AskQueueSpec, State) andalso
    bid_change_post(BidQueueSpec, State);
resume_post(State, _, _) ->
    timeout_post(State).

sys_next(#state{sys=running} = State) ->
    timeout_next(State);
sys_next(#state{sys=suspended} = State) ->
    State.

sys_post(#state{sys=running} = State) ->
    timeout_post(State);
sys_post(#state{sys=suspended}) ->
    true.

%% Helpers

bid_next(State, Fun) ->
    NState = bid_aqm_next(State),
    Fun(NState).

bid_post(State, Fun) ->
    {Drops, NState} = bid_aqm(State),
    Fun(NState) andalso drops_post(Drops).

ask_next(State, Fun) ->
    NState = ask_aqm_next(State),
    Fun(NState).

ask_post(State, Fun) ->
    {Drops, NState} = ask_aqm(State),
    Fun(NState) andalso drops_post(Drops).

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

bid_aqm_post(State) ->
    {Drops, _} = bid_aqm(State),
    drops_post(Drops).

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

ask_aqm_post(State) ->
    {Drops, _} = ask_aqm(State),
    drops_post(Drops).

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
            -RelSojournTime1 =:= RelSojournTime2 andalso
            (SojournTime2 - RelSojournTime2) =:= SojournTime1 andalso
            (SojournTime1 - RelSojournTime1) =:= SojournTime2;
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
    {await, ARef, Broker} = sbroker:async_ask_r(Broker, self(), ARef),
    client_init(MRef, Broker, ARef, queued);
client_init(Broker, nb_bid) ->
    MRef = monitor(process, Broker),
    State = sbroker:nb_ask_r(Broker),
    client_init(MRef, Broker, undefined, State);
client_init(Broker, async_ask) ->
    MRef = monitor(process, Broker),
    ARef = make_ref(),
    {await, ARef, Broker} = sbroker:async_ask(Broker, self(), ARef),
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
