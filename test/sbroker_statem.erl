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
-export([cancel/1]).
-export([bad_cancel/2]).
-export([change_config/2]).
-export([shutdown_client/1]).

-export([client_init/2]).

-record(state, {sbroker, asks=[], ask_out, ask_drops, ask_state=[], ask_size,
                ask_drop, bids=[], bid_out, bid_drops, bid_state=[], bid_size,
                bid_drop, cancels=[]}).

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
               {4, {call, sbroker, force_timeout,
                    force_timeout_args(State)}}] ++
              [{3, {call, ?MODULE, cancel, cancel_args(State)}} ||
                    cancel_command(State)] ++
              [{2, {call, ?MODULE, change_config,
                    change_config_args(State)}}] ++
              [{2, {call, ?MODULE, shutdown_client,
                    shutdown_client_args(State)}} ||
                    shutdown_client_command(State)] ++
              [{1, {call, ?MODULE, bad_cancel, bad_cancel_args(State)}}]).

precondition(State, {call, _, start_link, Args}) ->
    start_link_pre(State, Args);
precondition(#state{sbroker=undefined}, _) ->
    false;
precondition(State, {call, _, cancel, Args}) ->
    cancel_pre(State, Args);
precondition(State, {call, _, shutdown_client, Args}) ->
    shutdown_client_pre(State, Args);
precondition(_State, _Call) ->
    true.

next_state(State, Value, {call, _, start_link, Args}) ->
    start_link_next(State, Value, Args);
next_state(State, Value, {call, _, spawn_client, Args}) ->
    spawn_client_next(State, Value, Args);
next_state(State, Value, {call, _, force_timeout, Args}) ->
    force_timeout_next(State, Value, Args);
next_state(State, Value, {call, _, cancel, Args}) ->
    cancel_next(State, Value, Args);
next_state(State, Value, {call, _, bad_cancel, Args}) ->
    bad_cancel_next(State, Value, Args);
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
postcondition(State, {call, _, force_timeout, Args}, Result) ->
    force_timeout_post(State, Args, Result);
postcondition(State, {call, _, cancel, Args}, Result) ->
    cancel_post(State, Args, Result);
postcondition(State, {call, _, bad_cancel, Args}, Result) ->
    bad_cancel_post(State, Args, Result);
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


spawn_client_next(#state{asks=[]} = State, Bid, [_, async_bid]) ->
    #state{bids=Bids} = NState = bid_drop_state(State),
    after_next(NState#state{bids=Bids ++ [Bid]});
spawn_client_next(#state{asks=[]} = State, _, [_, nb_bid]) ->
    State;
spawn_client_next(#state{asks=[_ | _], ask_out=AskOut} = State, Value,
                  [_, BidFun] = Args)
  when BidFun =:= async_bid orelse BidFun =:= nb_bid ->
    case ask_drop_state(State) of
        #state{asks=[]} = NState ->
            spawn_client_next(NState, Value, Args);
        #state{asks=NAsks} = NState when AskOut =:= out ->
            after_next(NState#state{asks=dropfirst(NAsks)});
        #state{asks=NAsks} = NState when AskOut =:= out_r ->
            after_next(NState#state{asks=droplast(NAsks)})
    end;
spawn_client_next(#state{bids=[]} = State, Ask, [_, async_ask]) ->
    #state{asks=Asks} = NState = ask_drop_state(State),
    after_next(NState#state{asks=Asks ++ [Ask]});
spawn_client_next(#state{bids=[]} = State, _, [_, nb_ask]) ->
    State;
spawn_client_next(#state{bids=[_ | _], bid_out=BidOut} = State, Value,
                  [_, AskFun] = Args)
  when AskFun =:= async_ask orelse AskFun =:= nb_ask ->
    case bid_drop_state(State) of
        #state{bids=[]} = NState ->
            spawn_client_next(NState, Value, Args);
        #state{bids=NBids} = NState when BidOut =:= out ->
            after_next(NState#state{bids=dropfirst(NBids)});
        #state{bids=NBids} = NState when BidOut =:= out_r ->
            after_next(NState#state{bids=droplast(NBids)})
    end.

droplast([]) ->
    [];
droplast(List) ->
    [_ | Rest] = lists:reverse(List),
    lists:reverse(Rest).

dropfirst([]) ->
    [];
dropfirst([_ | Rest]) ->
    Rest.

bid_drop_state(State) ->
    {_, NState} = bid_drop(State),
    NState.

bid_drop(#state{bids=[]} = State) ->
    {[], State};
bid_drop(#state{bid_state=[], bid_drops=[]} = State) ->
    {[], State};
bid_drop(#state{bid_state=[], bid_drops=BidDrops} = State) ->
    bid_drop(State#state{bid_state=BidDrops});
bid_drop(#state{bids=Bids, bid_state=[BidDrop | NBidState]} = State) ->
    BidDrop2 = min(length(Bids), BidDrop),
    {Drops, NBids} = lists:split(BidDrop2, Bids),
    {Drops, State#state{bids=NBids, bid_state=NBidState}}.

ask_drop_state(State) ->
    {_, NState} = ask_drop(State),
    NState.

ask_drop(#state{asks=[]} = State) ->
    {[], State};
ask_drop(#state{ask_state=[], ask_drops=[]} = State) ->
    {[], State};
ask_drop(#state{ask_state=[], ask_drops=AskDrops} = State) ->
    ask_drop(State#state{ask_state=AskDrops});
ask_drop(#state{asks=Asks, ask_state=[AskDrop | NAskState]} = State) ->
    AskDrop2 = min(length(Asks), AskDrop),
    {Drops, NAsks} = lists:split(AskDrop2, Asks),
    {Drops, State#state{asks=NAsks, ask_state=NAskState}}.

spawn_client_post(#state{asks=[]} = State, [_, async_bid], Bid) ->
    {Drops, #state{bids=Bids} = NState} = bid_drop(State),
    drops_post(Drops) andalso after_post(NState#state{bids=Bids ++ [Bid]});
spawn_client_post(#state{asks=[]}, [_, nb_bid], Bid) ->
    retry_post(Bid);
spawn_client_post(#state{ask_out=AskOut} = State, [_, BidFun] = Args, Bid)
  when BidFun =:= async_bid orelse BidFun =:= nb_bid ->
    case ask_drop(State) of
        {Drops, #state{asks=[]} = NState} ->
            drops_post(Drops) andalso spawn_client_post(NState, Args, Bid);
        {Drops, #state{asks=Asks} = NState} when AskOut =:= out ->
            Ask = hd(Asks),
            drops_post(Drops) andalso settled_post(Bid, Ask) andalso
            after_post(NState#state{asks=dropfirst(Asks)});
        {Drops, #state{asks=Asks} = NState} when AskOut =:= out_r ->
            Ask = lists:last(Asks),
            drops_post(Drops) andalso settled_post(Bid, Ask) andalso
            after_post(NState#state{asks=droplast(Asks)})
    end;
spawn_client_post(#state{bids=[]} = State, [_, async_ask], Ask) ->
    {Drops, #state{asks=Asks} = NState} = ask_drop(State),
    drops_post(Drops) andalso after_post(NState#state{asks=Asks ++ [Ask]});
spawn_client_post(#state{bids=[]}, [_, nb_ask], Ask) ->
    retry_post(Ask);
spawn_client_post(#state{bid_out=BidOut} = State, [_, AskFun] = Args, Ask)
  when AskFun =:= async_ask orelse AskFun =:= nb_ask ->
    case bid_drop(State) of
        {Drops, #state{bids=[]} = NState} ->
            drops_post(Drops) andalso spawn_client_post(NState, Args, Ask);
        {Drops, #state{bids=Bids} = NState} when BidOut =:= out ->
            Bid = hd(Bids),
            drops_post(Drops) andalso settled_post(Ask, Bid) andalso
            after_post(NState#state{bids=dropfirst(Bids)});
        {Drops, #state{bids=Bids} = NState} when BidOut =:= out_r ->
            Bid = lists:last(Bids),
            drops_post(Drops) andalso settled_post(Ask, Bid) andalso
            after_post(NState#state{bids=droplast(Bids)})
    end.

drops_post([]) ->
    true;
drops_post([Client | Drops]) ->
    case result(Client) of
        {drop, _} ->
            drops_post(Drops);
        Other ->
            ct:log("~p Drop: ~p", [Client, Other]),
            false
    end.

settled_post(Client1, Client2) ->
    Pid1 = client_pid(Client1),
    Pid2 = client_pid(Client2),
    case {result(Client1), result(Client2)} of
        {{go, Ref, Pid2, 0}, {go, Ref, Pid1, _}} ->
            true;
        Result ->
            ct:log("Result: ~p", [Result]),
            false
    end.

retry_post(Client) ->
    case result(Client) of
        {retry, 0} ->
            true;
        Result ->
            ct:log("Result ~p", [Result]),
            false
    end.

result(Client) ->
    client_call(Client, result).

force_timeout_args(#state{sbroker=Broker}) ->
    [Broker].

force_timeout_next(#state{asks=[]} = State, _, _) ->
    after_next(bid_drop_state(State));
force_timeout_next(#state{bids=[]} = State, _, _) ->
    after_next(ask_drop_state(State)).

force_timeout_post(#state{asks=[]} = State, _, _) ->
    {Drops, NState} = bid_drop(State),
    drops_post(Drops) andalso after_post(NState);
force_timeout_post(#state{bids=[]} = State, _, _) ->
    {Drops, NState} = ask_drop(State),
    drops_post(Drops) andalso after_post(NState).

cancel_command(#state{asks=[], bids=[]}) ->
    false;
cancel_command(_) ->
    true.

cancel_args(#state{asks=Asks, bids=Bids}) ->
    [elements(Asks ++ Bids)].

cancel(Client) ->
    client_call(Client, cancel).

cancel_pre(#state{asks=Asks, bids=Bids}, [Client]) ->
    lists:member(Client, Asks) orelse lists:member(Client, Bids).

cancel_next(#state{asks=[], cancels=Cancels} = State, _, [Client]) ->
    NState = #state{bids=Bids} = bid_drop_state(State),
    after_next(NState#state{bids=Bids--[Client], cancels=Cancels++[Client]});
cancel_next(#state{bids=[], cancels=Cancels} = State, _, [Client]) ->
    NState = #state{asks=Asks} = ask_drop_state(State),
    after_next(NState#state{asks=Asks--[Client], cancels=Cancels++[Client]}).

cancel_post(#state{asks=[], bids=Bids} = State, [Client], 1) ->
    {Drops, NState} = bid_drop(State),
    drops_post(Drops) andalso after_post(NState#state{bids=Bids--[Client]});
cancel_post(#state{bids=[], asks=Asks} = State, [Client], 1) ->
    {Drops, NState} = ask_drop(State),
    drops_post(Drops) andalso after_post(NState#state{asks=Asks--[Client]});
cancel_post(#state{asks=[]} = State, [Client], false) ->
    {Drops, NState} = bid_drop(State),
    drops_post(Drops) andalso lists:member(Client, Drops) andalso
    after_post(NState);
cancel_post(#state{bids=[]} = State, [Client], false) ->
    {Drops, NState} = ask_drop(State),
    drops_post(Drops) andalso lists:member(Client, Drops) andalso
    after_post(NState);
cancel_post(_, _, _) ->
    false.

bad_cancel_args(#state{sbroker=Broker}) ->
    [Broker, make_ref()].

bad_cancel(Broker, Ref) ->
    sbroker:cancel(Broker, Ref).

bad_cancel_next(#state{asks=[]} = State, _, _) ->
    after_next(bid_drop_state(State));
bad_cancel_next(#state{bids=[]} = State, _, _) ->
    after_next(ask_drop_state(State)).

bad_cancel_post(#state{asks=[]} = State, _, false) ->
    {Drops, NState} = bid_drop(State),
    drops_post(Drops) andalso after_post(NState);
bad_cancel_post(#state{bids=[]} = State, _, false) ->
    {Drops, NState} = ask_drop(State),
    drops_post(Drops) andalso after_post(NState);
bad_cancel_post(_, _, _) ->
    false.

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

shutdown_client_command(#state{asks=[], bids=[], cancels=[]}) ->
    false;
shutdown_client_command(_) ->
    true.

shutdown_client_args(#state{asks=Asks, bids=Bids, cancels=Cancels}) ->
    [elements(Asks ++ Bids ++ Cancels)].

shutdown_client(Client) ->
    Pid = client_pid(Client),
    MRef = monitor(process, Pid),
    exit(Pid, shutdown),
    receive
        {'DOWN', MRef, _, _, shutdown} ->
            ok;
        {'DOWN', MRef, _, _, Reason} ->
            exit(Reason)
    after
        100 ->
            exit(timeout)
    end.

shutdown_client_pre(#state{asks=Asks, bids=Bids, cancels=Cancels}, [Client]) ->
    lists:member(Client, Asks) orelse lists:member(Client, Bids) orelse
    lists:member(Client, Cancels).

shutdown_client_next(#state{asks=Asks, bids=Bids, cancels=Cancels} = State, _,
                     [Client]) ->
    case lists:member(Client, Cancels) of
        true ->
            after_next(State#state{cancels=Cancels--[Client]});
        false when Asks =:= [] ->
            NState = #state{bids=NBids} = bid_drop_state(State),
            after_next(NState#state{bids=NBids--[Client]});
        false when Bids =:= [] ->
            NState = #state{asks=NAsks} = ask_drop_state(State),
            after_next(NState#state{asks=NAsks--[Client]})
    end.

shutdown_client_post(#state{asks=Asks, bids=Bids, cancels=Cancels} = State,
                     [Client], _) ->
    case lists:member(Client, Cancels) of
        true ->
            true;
        false when Asks =:= [] ->
            {Drops, #state{bids=NBids} = NState} = bid_drop(State),
            drops_post(Drops--[Client]) andalso
            after_post(NState#state{bids=NBids--[Client]});
        false when Bids =:= [] ->
            {Drops, #state{asks=NAsks} = NState} = ask_drop(State),
            drops_post(Drops--[Client]) andalso
            after_post(NState#state{asks=NAsks--[Client]})
    end.

%% after

after_next(State) ->
    NState = after_next_ask(State),
    after_next_bid(NState).

after_next_ask(#state{asks=Asks, ask_size=AskSize, ask_drop=AskDrop} = State)
  when length(Asks) > AskSize ->
    #state{asks=NAsks} = NState = ask_drop_state(State),
    case AskDrop of
        drop ->
            after_next_ask(NState#state{asks=dropfirst(NAsks)});
        drop_r ->
            after_next_ask(NState#state{asks=droplast(NAsks)})
    end;
after_next_ask(State) ->
    State.

after_next_bid(#state{bids=Bids, bid_size=BidSize, bid_drop=BidDrop} = State)
  when length(Bids) > BidSize ->
    #state{bids=NBids} = NState = bid_drop_state(State),
    case BidDrop of
        drop ->
            after_next_bid(NState#state{bids=dropfirst(NBids)});
        drop_r ->
            after_next_bid(NState#state{bids=droplast(NBids)})
    end;
after_next_bid(State) ->
    State.

after_post(State) ->
    after_post_ask(State) andalso after_post_bid(State).

after_post_ask(#state{asks=Asks, ask_size=AskSize, ask_drop=AskDrop} = State)
  when length(Asks) > AskSize ->
    {Drops, #state{asks=NAsks} = NState} = ask_drop(State),
    case AskDrop of
        drop ->
            drops_post(Drops) andalso
            after_post_ask(NState#state{asks=dropfirst(NAsks)});
        drop_r ->
            drops_post(Drops) andalso
            after_post_ask(NState#state{asks=droplast(NAsks)})
    end;
after_post_ask(_) ->
    true.

after_post_bid(#state{bids=Bids, bid_size=BidSize, bid_drop=BidDrop} = State)
  when length(Bids) > BidSize ->
    {Drops, #state{bids=NBids} = NState} = bid_drop(State),
    case BidDrop of
        drop ->
            drops_post(Drops) andalso
            after_post_bid(NState#state{bids=dropfirst(NBids)});
        drop_r ->
            drops_post(Drops) andalso
            after_post_bid(NState#state{bids=droplast(NBids)})
    end;
after_post_bid(_) ->
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
            case sbroker:cancel(Broker, ARef) of
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
