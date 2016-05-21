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
-export([get_modules/2]).

-export([client_init/2]).

-record(state, {sbroker, asks=[], ask_mod, ask_out, ask_drops, ask_state=[],
                bids=[], bid_mod, bid_out, bid_drops, bid_state=[], meter_mod,
                cancels=[], done=[], sys=running, change}).

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
               {1, {call, sys, suspend, suspend_args(State)}},
               {1, {call, ?MODULE, get_modules, get_modules_args(State)}}]);
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
next_state(State, Value, {call, _, get_modules, Args}) ->
    get_modules_next(State, Value, Args);
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
postcondition(State, {call, _, get_modules, Args}, Result) ->
    get_modules_post(State, Args, Result);
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
    frequency([{30, {ok, {queue_spec(), queue_spec(), meter_spec()}}},
               {1, ignore},
               {1, bad}]).

queue_spec() ->
    {oneof([sbroker_statem_queue, sbroker_statem2_queue]),
     {oneof([out, out_r]), resize(4, list(oneof([0, choose(1, 2)])))}}.

meter_spec() ->
    oneof([{sbroker_alarm_meter, {0, 1000, ?MODULE}},
           {sbroker_timeout_meter, oneof([1000, infinity])},
           {sbroker_statem_meter, self}]).

start_link(Init) ->
    application:set_env(sbroker, ?MODULE, update_spec(Init)),
    Trap = process_flag(trap_exit, true),
    case sbroker:start_link(?MODULE, [], [{read_time_after, 2}]) of
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

update_spec({ok, {AskSpec, BidSpec, {sbroker_statem_meter, self}}}) ->
    {ok, {AskSpec, BidSpec, {sbroker_statem_meter, self()}}};
update_spec(Other) ->
    Other.

init([]) ->
    {ok, Result} = application:get_env(sbroker, ?MODULE),
    Result.

start_link_pre(#state{sbroker=Broker}, _) ->
    Broker =:= undefined.

start_link_next(State, Value,
                [{ok, {{AskMod, {AskOut, AskDrops}},
                       {BidMod, {BidOut, BidDrops}},
                       {MeterMod, _}}}]) ->
    Broker = {call, erlang, element, [2, Value]},
    State#state{sbroker=Broker, bid_mod=BidMod, bid_out=BidOut,
                bid_drops=BidDrops, bid_state=BidDrops, ask_mod=AskMod,
                ask_out=AskOut, ask_drops=AskDrops, ask_state=AskDrops,
                meter_mod=MeterMod};
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

spawn_client_next(#state{asks=[], ask_drops=AskDrops, bids=Bids} = State, Bid,
                  [_, async_bid]) ->
    bid_next(State#state{ask_state=AskDrops, bids=Bids++[Bid]});
spawn_client_next(#state{asks=[], ask_drops=AskDrops} = State, _,
                  [_, nb_bid]) ->
    timeout_next(State#state{ask_state=AskDrops});
spawn_client_next(#state{asks=[_ | _]} = State, Bid, [_, BidFun] = Args)
  when BidFun =:= async_bid orelse BidFun =:= nb_bid ->
    ask_next(State,
             fun(#state{asks=[], ask_drops=AskDrops} = NState) ->
                     NState2 = NState#state{ask_state=AskDrops},
                     spawn_client_next(NState2, Bid, Args);
                (#state{asks=NAsks, ask_out=out, done=Done} = NState) ->
                     NState#state{asks=dropfirst(NAsks),
                                  done=Done++[Bid, hd(NAsks)]};
                (#state{asks=NAsks, ask_out=out_r, done=Done} = NState) ->
                     NState#state{asks=droplast(NAsks),
                                  done=Done++[Bid, lists:last(NAsks)]}
             end);
spawn_client_next(#state{bids=[], bid_drops=BidDrops, asks=Asks} = State, Ask,
                  [_, async_ask]) ->
    ask_next(State#state{bid_state=BidDrops, asks=Asks++[Ask]});
spawn_client_next(#state{bids=[], bid_drops=BidDrops} = State, _,
                  [_, nb_ask]) ->
    timeout_next(State#state{bid_state=BidDrops});
spawn_client_next(#state{bids=[_ | _]} = State, Ask, [_, AskFun] = Args)
  when AskFun =:= async_ask orelse AskFun =:= nb_ask ->
    bid_next(State,
             fun(#state{bids=[], bid_drops=BidDrops} = NState) ->
                     NState2 = NState#state{bid_state=BidDrops},
                     spawn_client_next(NState2, Ask, Args);
                (#state{bids=NBids, bid_out=out, done=Done} = NState) ->
                     NState#state{bids=dropfirst(NBids),
                                  done=Done++[Ask, hd(NBids)]};
                (#state{bids=NBids, bid_out=out_r, done=Done} = NState) ->
                     NState#state{bids=droplast(NBids),
                                  done=Done++[Ask, lists:last(NBids)]}
             end).

spawn_client_post(#state{asks=[], ask_drops=AskDrops, bids=Bids} = State,
                  [_, async_bid], Bid) ->
    bid_post(State#state{ask_state=AskDrops, bids=Bids++[Bid]},
             fun meter_post/1);
spawn_client_post(#state{asks=[], ask_drops=AskDrops} = State,
                  [_, nb_bid], Bid) ->
    retry_post(Bid) andalso timeout_post(State#state{ask_state=AskDrops});
spawn_client_post(#state{asks=[_ | _]} = State, [_, BidFun] = Args, Bid)
  when BidFun =:= async_bid orelse BidFun =:= nb_bid ->
    ask_post(State,
             fun(#state{asks=[], ask_drops=AskDrops} = NState) ->
                     NState2 = NState#state{ask_state=AskDrops},
                     spawn_client_post(NState2, Args, Bid);
                (#state{asks=NAsks, ask_out=out} = NState) ->
                     settled_post(NState, hd(NAsks), Bid);
                (#state{asks=NAsks, ask_out=out_r} = NState) ->
                     settled_post(NState, lists:last(NAsks), Bid)
             end);
spawn_client_post(#state{bids=[], bid_drops=BidDrops, asks=Asks} = State,
                  [_, async_ask], Ask) ->
    ask_post(State#state{bid_state=BidDrops, asks=Asks++[Ask]},
             fun meter_post/1);
spawn_client_post(#state{bids=[], bid_drops=BidDrops} = State,
                  [_, nb_ask], Ask) ->
    retry_post(Ask) andalso timeout_post(State#state{bid_state=BidDrops});
spawn_client_post(#state{bids=[_ | _]} = State, [_, AskFun] = Args, Ask)
  when AskFun =:= async_ask orelse AskFun =:= nb_ask ->
    bid_post(State,
             fun(#state{bids=[], bid_drops=BidDrops} = NState) ->
                     NState2 = NState#state{bid_state=BidDrops},
                     spawn_client_post(NState2, Args, Ask);
                (#state{bids=NBids, bid_out=out} = NState) ->
                     settled_post(NState, Ask, hd(NBids));
                (#state{bids=NBids, bid_out=out_r} = NState) ->
                     settled_post(NState, Ask, lists:last(NBids))
             end).

timeout_args(#state{sbroker=Broker}) ->
    [Broker].

timeout_next(State, _, _) ->
    timeout_next(State).

timeout_next(#state{asks=[]} = State) ->
    bid_next(State);
timeout_next(#state{bids=[]} = State) ->
    ask_next(State).

timeout_post(State, _, _) ->
    timeout_post(State).

timeout_post(#state{asks=[]} = State) ->
    bid_post(State, fun meter_post/1);
timeout_post(#state{bids=[]} = State) ->
    ask_post(State, fun meter_post/1).

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

cancel_next(#state{asks=[], bids=Bids, cancels=Cancels} = State, _,
            [_, Client]) ->
    case lists:member(Client, Bids) of
        true ->
            bid_next(State#state{bids=Bids--[Client],
                                 cancels=Cancels++[Client]});
        false ->
            bid_next(State)
    end;
cancel_next(#state{bids=[], asks=Asks, cancels=Cancels} = State, _,
            [_, Client]) ->
    case lists:member(Client, Asks) of
        true ->
            ask_next(State#state{asks=Asks--[Client],
                                 cancels=Cancels++[Client]});
        false ->
            ask_next(State)
    end.

cancel_post(#state{asks=[], bids=Bids} = State, [_, Client], Result) ->
    case lists:member(Client, Bids) of
        true when Result =:= 1 ->
            bid_post(State#state{bids=Bids--[Client]}, fun meter_post/1);
        true ->
            ct:pal("Cancel: ~p", [Result]),
            false;
        false when Result =:= false ->
            bid_post(State, fun meter_post/1);
        false ->
            ct:pal("Cancel: ~p", [Result]),
            false
    end;
cancel_post(#state{bids=[], asks=Asks} = State, [_, Client], Result) ->
    case lists:member(Client, Asks) of
        true when Result =:= 1 ->
            ask_post(State#state{asks=Asks--[Client]}, fun meter_post/1);
        true ->
            ct:pal("Cancel: ~p", [Result]),
            false;
        false when Result =:= false ->
            ask_post(State, fun meter_post/1);
        false ->
            ct:pal("Cancel: ~p", [Result]),
            false
    end.

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
    application:set_env(sbroker, ?MODULE, update_spec(Init)),
    sbroker:change_config(Broker, 100).

change_config_next(State, _, [_, ignore]) ->
    timeout_next(State);
change_config_next(State, _, [_, bad]) ->
    timeout_next(State);
change_config_next(State, _,
                   [_, {ok, {AskQueueSpec, BidQueueSpec, {MeterMod, _}}}]) ->
    NState = ask_change_next(AskQueueSpec, State#state{meter_mod=MeterMod}),
    bid_change_next(BidQueueSpec, NState).

%% If module and args the same state not changed by backends.
ask_change_next({AskMod, {AskOut, AskDrops}},
           #state{ask_mod=AskMod, ask_drops=AskDrops} = State) ->
    ask_aqm_next(State#state{ask_out=AskOut});
%% If module and/or args different reset the backend state.
ask_change_next({AskMod, {AskOut, AskDrops}}, State) ->
    ask_aqm_next(State#state{ask_mod=AskMod, ask_out=AskOut, ask_drops=AskDrops,
                             ask_state=AskDrops}).

bid_change_next({BidMod, {BidOut, BidDrops}},
           #state{bid_mod=BidMod, bid_drops=BidDrops} = State) ->
    bid_aqm_next(State#state{bid_out=BidOut});
bid_change_next({BidMod, {BidOut, BidDrops}}, State) ->
    bid_aqm_next(State#state{bid_mod=BidMod, bid_out=BidOut, bid_drops=BidDrops,
                             bid_state=BidDrops}).

change_config_post(State, [_, ignore], ok) ->
    timeout_post(State);
change_config_post(State, [_, bad], {error, {bad_return_value, bad}}) ->
    timeout_post(State);
change_config_post(State,
                   [_, {ok, {AskQueueSpec, BidQueueSpec, {MeterMod, _}}}],
                   ok) ->
    ask_change_post(AskQueueSpec, State) andalso
    bid_change_post(BidQueueSpec, State) andalso
    meter_post(State#state{meter_mod=MeterMod});
change_config_post(_, _, _) ->
    false.

ask_change_post({AskMod, {AskOut, AskDrops}},
           #state{ask_mod=AskMod, ask_drops=AskDrops} = State) ->
    ask_aqm_post(State#state{ask_out=AskOut});
%% If module and/or args different reset the backend state.
ask_change_post({AskMod, {AskOut, AskDrops}}, State) ->
    ask_aqm_post(State#state{ask_mod=AskMod, ask_out=AskOut, ask_drops=AskDrops,
                             ask_state=AskDrops}).

bid_change_post({BidMod, {BidOut, BidDrops}},
                #state{bid_mod=BidMod, bid_drops=BidDrops} = State) ->
    bid_aqm_post(State#state{bid_out=BidOut});
bid_change_post({BidMod, {BidOut, BidDrops}}, State) ->
    bid_aqm_post(State#state{bid_mod=BidMod, bid_out=BidOut, bid_drops=BidDrops,
                             bid_state=BidDrops}).

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
shutdown_client_next(#state{asks=[], bids=Bids, cancels=Cancels,
                            done=Done} = State, _, [_, Client]) ->
    case lists:member(Client, Bids) of
        true ->
            bid_aqm_next(State#state{bids=Bids--[Client], done=Done--[Client]});
        false ->
            State#state{cancels=Cancels--[Client], done=Done--[Client]}
    end;
shutdown_client_next(#state{bids=[], asks=Asks, cancels=Cancels,
                            done=Done} = State, _, [_, Client]) ->
    case lists:member(Client, Asks) of
        true ->
            ask_aqm_next(State#state{asks=Asks--[Client], done=Done--[Client]});
        false ->
            State#state{cancels=Cancels--[Client], done=Done--[Client]}
    end.

shutdown_client_post(#state{asks=[]} = State, [_, undefined], _) ->
    bid_post(State, fun meter_post/1);
shutdown_client_post(#state{bids=[]} = State, [_, undefined], _) ->
    ask_post(State, fun meter_post/1);
shutdown_client_post(#state{asks=[], bids=Bids} = State, [_, Client], _) ->
    case lists:member(Client, Bids) of
        true ->
            bid_post(State#state{bids=Bids--[Client]}, fun meter_post/1);
        false ->
            true
    end;
shutdown_client_post(#state{bids=[], asks=Asks} = State, [_, Client], _) ->
    case lists:member(Client, Asks) of
        true ->
            ask_post(State#state{asks=Asks--[Client]}, fun meter_post/1);
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
                     {"Time", {TimeMod, Time}}]},
             {items, {"Installed handlers", Handlers}}]) ->
    SysState =:= Sys andalso Self =:= self() andalso
    ((length(Asks) > 0 andalso Active =:= ask) orelse
     (length(Bids) > 0 andalso Active =:= ask_r) orelse
     (Asks =:= [] andalso Bids =:= [])) andalso
    ((erlang:function_exported(erlang, monotonic_time, 1) andalso
      TimeMod =:= erlang) orelse TimeMod =:= sbroker_legacy) andalso
    is_integer(Time) andalso
    get_state_post(State, Handlers);
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

get_state_post(State, Handlers) ->
    get_ask_post(State, Handlers) andalso get_bid_post(State, Handlers).

get_ask_post(#state{ask_mod=AskMod, asks=Asks}, Handlers) ->
    case lists:keyfind(ask, 2, Handlers) of
        {AskMod, ask, AskState} ->
            AskMod:len(AskState) == length(Asks);
        Ask ->
            ct:pal("Ask queue: ~p", [Ask]),
            false
    end.

get_bid_post(#state{bid_mod=BidMod, bids=Bids}, Handlers) ->
    case lists:keyfind(ask_r, 2, Handlers) of
        {BidMod, ask_r, BidState} ->
            BidMod:len(BidState) == length(Bids);
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

get_modules_args(#state{sbroker=Broker}) ->
    [Broker, ?TIMEOUT].

get_modules(Broker, Timeout) ->
    {ok, Mods} = gen:call(Broker, self(), get_modules, Timeout),
    Mods.

get_modules_next(State, _, _) ->
    timeout_next(State).

get_modules_post(#state{ask_mod=AskMod, bid_mod=BidMod,
                        meter_mod=MeterMod} = State, _, Result) ->
    case lists:usort([?MODULE, AskMod, BidMod, MeterMod]) of
        Result ->
            timeout_post(State);
        Mods ->
            ct:pal("Modules~nExpected: ~p~nObserved: ~p", [Mods, Result]),
            false
    end.

change_code_args(#state{sbroker=Broker}) ->
    Mod = oneof([{?MODULE, init()},
                 sbroker_statem_queue,
                 sbroker_statem2_queue,
                 sbroker_alarm_meter,
                 sbroker_timeout_meter,
                 sbroker_statem_meter]),
    [Broker, Mod, ?TIMEOUT].

change_code(Broker, {?MODULE, Init}, Timeout) ->
    application:set_env(sbroker, ?MODULE, update_spec(Init)),
    sys:change_code(Broker, ?MODULE, undefined, undefined, Timeout);
change_code(Broker, Mod, Timeout) ->
    sys:change_code(Broker, Mod, undefined, undefined, Timeout).

change_code_pre(#state{sys=SysState}, _) ->
    SysState =:= suspended.

change_code_next(State, _, [_, {?MODULE, {ok, _} = Init}, _]) ->
    State#state{change=Init};
change_code_next(State, {?MODULE, _}, _) ->
    State;
change_code_next(State, _, [_, Mod, _]) ->
    change_code_bid(Mod, change_code_ask(Mod, State)).

change_code_ask(Mod, #state{ask_mod=Mod, ask_drops=AskDrops} = State) ->
    State#state{ask_state=AskDrops};
change_code_ask(_, State) ->
    State.

change_code_bid(Mod, #state{bid_mod=Mod, bid_drops=BidDrops} = State) ->
    State#state{bid_state=BidDrops};
change_code_bid(_, State) ->
    State.

change_code_post(_, [_, {?MODULE, {ok, _}}, _], ok) ->
    true;
change_code_post(_, [_, {?MODULE, ignore}, _], ok) ->
    true;
change_code_post(_, [_, {?MODULE, bad}, _],
                 {error, {bad_return_value, bad}}) ->
    true;
change_code_post(_, [_, Mod, _], ok) when is_atom(Mod) ->
    true;
change_code_post(_, _, _) ->
    false.

resume_args(#state{sbroker=Broker}) ->
    [Broker, ?TIMEOUT].

resume_pre(#state{sys=SysState}, _) ->
    SysState =:= suspended.

resume_next(#state{change={ok, {AskQueueSpec, BidQueueSpec,
                                {MeterMod, _}}}} = State, _, _) ->
    NState = State#state{sys=running, change=undefined, meter_mod=MeterMod},
    NState2 = ask_change_next(AskQueueSpec, NState),
    bid_change_next(BidQueueSpec, NState2);
resume_next(State, _, _) ->
    timeout_next(State#state{sys=running}).

resume_post(#state{change={ok, {AskQueueSpec, BidQueueSpec,
                                {MeterMod, _}}}} = State, _, _) ->
    NState = State#state{sys=running, change=undefined, meter_mod=MeterMod},
    ask_change_post(AskQueueSpec, NState) andalso
    bid_change_post(BidQueueSpec, NState) andalso meter_post(NState);
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

meter_post(#state{meter_mod=sbroker_statem_meter, asks=Asks, bids=Bids}) ->
    receive
        {meter, QueueDelay, ProcessDelay, RelativeTime, _}
          when QueueDelay >= 0, ProcessDelay >= 0 ->
            relative_post(RelativeTime, Asks, Bids)
    after
        100 ->
            ct:pal("Did not receive sbroker_statem_meter message"),
            false
    end;
meter_post(_) ->
    true.

relative_post(RelativeTime, [_|_] = Asks, _) when RelativeTime < 0 ->
    ct:pal("RelativeTime is negative ~p when asks waiting ~p",
           [RelativeTime, Asks]),
    false;
relative_post(RelativeTime, _, [_|_] = Bids) when RelativeTime > 0 ->
    ct:pal("RelativeTime is positive ~p when bids waiting ~p",
           [RelativeTime, Bids]),
    false;
relative_post(_, _, _) ->
    true.

%% Helpers

bid_next(State) ->
    bid_next(State, fun(NState) -> NState end).

bid_next(State, Fun) ->
    NState = bid_aqm_next(State),
    Fun(NState).

bid_post(State, Fun) ->
    {Drops, NState} = bid_aqm(State),
    Fun(NState) andalso drops_post(Drops).

ask_next(State) ->
    ask_next(State, fun(NState) -> NState end).

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

settled_post(#state{asks=Asks, bids=Bids} = State, AskClient, BidClient) ->
    AskPid = client_pid(AskClient),
    BidPid = client_pid(BidClient),
    case {result(AskClient), result(BidClient)} of
        {{go, Ref, BidPid, AskRel, AskSojourn},
         {go, Ref, AskPid, BidRel, BidSojourn}} when
            is_integer(AskSojourn) andalso AskSojourn >= 0 andalso
            is_integer(BidSojourn) andalso BidSojourn >= 0 andalso
            ((Asks == [] andalso is_integer(AskRel) andalso AskRel =< 0) orelse
             (Bids == [] andalso is_integer(AskRel) andalso AskRel >= 0))
            andalso
            -BidRel =:= AskRel andalso
            (AskSojourn - AskRel) =:= BidSojourn andalso
            (BidSojourn - BidRel) =:= AskSojourn ->
            meter_post(State);
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
