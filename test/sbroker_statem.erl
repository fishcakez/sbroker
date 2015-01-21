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

-export([spawn_client/2]).
-export([cancel/1]).
-export([bad_cancel/2]).
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

command(#state{sbroker=undefined}) ->
    {call, sbroker, start_link, start_link_args()};
command(State) ->
    frequency([{10, {call, ?MODULE, spawn_client,
                     spawn_client_args(State)}},
               {4, {call, sbroker, force_timeout,
                    force_timeout_args(State)}}] ++
              [{3, {call, ?MODULE, cancel, cancel_args(State)}} ||
                    cancel_command(State)] ++
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
postcondition(State, {call, _, shutdown_client, Args}, Result) ->
    shutdown_client_post(State, Args, Result);
postcondition(_State, _Call, _Result) ->
    true.

cleanup(#state{sbroker=undefined}) ->
    ok;
cleanup(#state{sbroker=Broker}) ->
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

start_link_args() ->
    [queue_spec(), queue_spec(), 10000].

queue_spec() ->
    {sbroker_statem_queue, list(oneof([0, choose(1, 2)])), oneof([out, out_r]),
     oneof([choose(0, 5), infinity]), oneof([drop, drop_r])}.

start_link_pre(#state{sbroker=Broker}, _) ->
    Broker =:= undefined.

start_link_next(State, Value, [{_, AskDrops, AskOut, AskSize, AskDrop},
                               {_, BidDrops, BidOut, BidSize, BidDrop}, _]) ->
    Broker = {call, erlang, element, [2, Value]},
    State#state{sbroker=Broker, bid_out=BidOut, bid_drops=BidDrops,
                bid_state=BidDrops, bid_size=BidSize, bid_drop=BidDrop,
                ask_out=AskOut, ask_drops=AskDrops, ask_state=AskDrops,
                ask_size=AskSize, ask_drop=AskDrop}.

start_link_post(_, _, {ok, Broker}) when is_pid(Broker) ->
    true;
start_link_post(_, _, _) ->
    false.

spawn_client_args(#state{sbroker=Broker}) ->
    [Broker, oneof([async_bid, async_ask, nb_bid, nb_ask])].

spawn_client(Broker, Fun) ->
    {ok, Pid, MRef} = proc_lib:start(?MODULE, client_init, [Broker, Fun]),
    {Pid, MRef}.


spawn_client_next(#state{asks=[], bid_size=BidSize, bid_drop=BidDrop} = State,
                  Bid, [_, async_bid]) ->
    #state{bids=Bids} = NState = bid_drop_state(State),
    NState2 = NState#state{bids=Bids ++ [Bid]},
    case length(Bids) + 1 > BidSize of
        true when BidDrop =:= drop ->
            #state{bids=NBids} = NState3 = bid_drop_state(NState2),
            NState3#state{bids=dropfirst(NBids)};
        true when BidDrop =:= drop_r ->
            #state{bids=NBids} = NState3 = bid_drop_state(NState2),
            NState3#state{bids=droplast(NBids)};
        false ->
            NState2
    end;
spawn_client_next(#state{asks=[]} = State, _, [_, nb_bid]) ->
    State;
spawn_client_next(#state{asks=[_ | _], ask_out=AskOut} = State, Value,
                  [_, BidFun] = Args)
  when BidFun =:= async_bid orelse BidFun =:= nb_bid ->
    case ask_drop_state(State) of
        #state{asks=[]} = NState ->
            spawn_client_next(NState, Value, Args);
        #state{asks=NAsks} = NState when AskOut =:= out ->
            NState#state{asks=dropfirst(NAsks)};
        #state{asks=NAsks} = NState when AskOut =:= out_r ->
            NState#state{asks=droplast(NAsks)}
    end;
spawn_client_next(#state{bids=[], ask_size=AskSize, ask_drop=AskDrop} = State,
                  Ask, [_, async_ask]) ->
    #state{asks=Asks} = NState = ask_drop_state(State),
    NState2 = NState#state{asks=Asks ++ [Ask]},
    case length(Asks) + 1 > AskSize of
        true when AskDrop =:= drop ->
            #state{asks=NAsks} = NState3 = ask_drop_state(NState2),
            NState3#state{asks=dropfirst(NAsks)};
        true when AskDrop =:= drop_r ->
            #state{asks=NAsks} = NState3 = ask_drop_state(NState2),
            NState3#state{asks=droplast(NAsks)};
        false ->
            NState2
    end;
spawn_client_next(#state{bids=[]} = State, _, [_, nb_ask]) ->
    State;
spawn_client_next(#state{bids=[_ | _], bid_out=BidOut} = State, Value,
                  [_, AskFun] = Args)
  when AskFun =:= async_ask orelse AskFun =:= nb_ask ->
    case bid_drop_state(State) of
        #state{bids=[]} = NState ->
            spawn_client_next(NState, Value, Args);
        #state{bids=NBids} = NState when BidOut =:= out ->
            NState#state{bids=dropfirst(NBids)};
        #state{bids=NBids} = NState when BidOut =:= out_r ->
            NState#state{bids=droplast(NBids)}
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

spawn_client_post(#state{asks=[], bid_size=BidSize, bid_drop=BidDrop} = State,
                  [_, async_bid], Bid) ->
    {Drops, #state{bids=Bids} = NState} = bid_drop(State),
    NState2 = NState#state{bids=Bids ++ [Bid]},
    case length(Bids) + 1 > BidSize of
        true when BidDrop =:= drop ->
            {Drops2, #state{bids=NBids}}= bid_drop(NState2),
            drops_post(Drops) andalso drops_post(Drops2) andalso
            (NBids =:= [] orelse drops_post([hd(NBids)]));
        true when BidDrop =:= drop_r ->
            {Drops2, #state{bids=NBids}}= bid_drop(NState2),
            drops_post(Drops) andalso drops_post(Drops2) andalso
            (NBids =:= [] orelse drops_post([lists:last(NBids)]));
        false ->
            drops_post(Drops)
    end;
spawn_client_post(#state{asks=[]}, [_, nb_bid], Bid) ->
    retry_post(Bid);
spawn_client_post(#state{ask_out=AskOut} = State, [_, BidFun] = Args, Bid)
  when BidFun =:= async_bid orelse BidFun =:= nb_bid ->
    case ask_drop(State) of
        {Drops, #state{asks=[]} = NState} ->
            drops_post(Drops) andalso spawn_client_post(NState, Args, Bid);
        {Drops, #state{asks=Asks}} when AskOut =:= out ->
            Ask = hd(Asks),
            drops_post(Drops) andalso settled_post(Bid, Ask);
        {Drops, #state{asks=Asks}} when AskOut =:= out_r ->
            Ask = lists:last(Asks),
            drops_post(Drops) andalso settled_post(Bid, Ask)
    end;
spawn_client_post(#state{bids=[], ask_size=AskSize, ask_drop=AskDrop} = State,
                  [_, async_ask], Ask) ->
    {Drops, #state{asks=Asks} = NState} = ask_drop(State),
    NState2 = NState#state{asks=Asks ++ [Ask]},
    case length(Asks) + 1 > AskSize of
        true when AskDrop =:= drop ->
            {Drops2, #state{asks=NAsks}}= ask_drop(NState2),
            drops_post(Drops) andalso drops_post(Drops2) andalso
            (NAsks =:= [] orelse drops_post([hd(NAsks)]));
        true when AskDrop =:= drop_r ->
            {Drops2, #state{asks=NAsks}}= ask_drop(NState2),
            drops_post(Drops) andalso drops_post(Drops2) andalso
            (NAsks =:= [] orelse drops_post([lists:last(NAsks)]));
        false ->
            drops_post(Drops)
    end;
spawn_client_post(#state{bids=[]}, [_, nb_ask], Ask) ->
    retry_post(Ask);
spawn_client_post(#state{bid_out=BidOut} = State, [_, AskFun] = Args, Ask)
  when AskFun =:= async_ask orelse AskFun =:= nb_ask ->
    case bid_drop(State) of
        {Drops, #state{bids=[]} = NState} ->
            drops_post(Drops) andalso spawn_client_post(NState, Args, Ask);
        {Drops, #state{bids=Bids}} when BidOut =:= out ->
            Bid = hd(Bids),
            drops_post(Drops) andalso settled_post(Ask, Bid);
        {Drops, #state{bids=Bids}} when BidOut =:= out_r ->
            Bid = lists:last(Bids),
            drops_post(Drops) andalso settled_post(Ask, Bid)
    end;
spawn_client_post(#state{bid_out=BidOut} = State, [_, nb_ask], Ask) ->
    case bid_drop(State) of
        {Drops, #state{bids=[]}} ->
            drops_post(Drops);
        {Drops, #state{bids=Bids}} when BidOut =:= out ->
            Bid = hd(Bids),
            drops_post(Drops) andalso settled_post(Ask, Bid);
        {Drops, #state{bids=Bids}} when BidOut =:= out_r ->
            Bid = lists:last(Bids),
            drops_post(Drops) andalso settled_post(Ask, Bid)
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
    bid_drop_state(State);
force_timeout_next(#state{bids=[]} = State, _, _) ->
    ask_drop_state(State).

force_timeout_post(#state{asks=[]} = State, _, _) ->
    {Drops, _} = bid_drop(State),
    drops_post(Drops);
force_timeout_post(#state{bids=[]} = State, _, _) ->
    {Drops, _} = ask_drop(State),
    drops_post(Drops).

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
    NState#state{bids=Bids--[Client], cancels=Cancels++[Client]};
cancel_next(#state{bids=[], cancels=Cancels} = State, _, [Client]) ->
    NState = #state{asks=Asks} = ask_drop_state(State),
    NState#state{asks=Asks--[Client], cancels=Cancels++[Client]}.

cancel_post(#state{asks=[]} = State, _, ok) ->
    {Drops, _} = bid_drop(State),
    drops_post(Drops);
cancel_post(#state{bids=[]} = State, _, ok) ->
    {Drops, _} = ask_drop(State),
    drops_post(Drops);
cancel_post(#state{asks=[]} = State, [Client], {error, not_found}) ->
    {Drops, _} = bid_drop(State),
    drops_post(Drops) andalso lists:member(Client, Drops);
cancel_post(#state{bids=[]} = State, [Client], {error, not_found}) ->
    {Drops, _} = ask_drop(State),
    drops_post(Drops) andalso lists:member(Client, Drops).

bad_cancel_args(#state{sbroker=Broker}) ->
    [Broker, make_ref()].

bad_cancel(Broker, Ref) ->
    sbroker:cancel(Broker, Ref).

bad_cancel_next(#state{asks=[]} = State, _, _) ->
    bid_drop_state(State);
bad_cancel_next(#state{bids=[]} = State, _, _) ->
    ask_drop_state(State).

bad_cancel_post(#state{asks=[]} = State, _, {error, not_found}) ->
    {Drops, _} = bid_drop(State),
    drops_post(Drops);
bad_cancel_post(#state{bids=[]} = State, _, {error, not_found}) ->
    {Drops, _} = ask_drop(State),
    drops_post(Drops);
bad_cancel_post(_, _, _) ->
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
            State#state{cancels=Cancels--[Client]};
        false when Asks =:= [] ->
            NState = #state{bids=NBids} = bid_drop_state(State),
            NState#state{bids=NBids--[Client]};
        false when Bids =:= [] ->
            NState = #state{asks=NAsks} = ask_drop_state(State),
            NState#state{asks=NAsks--[Client]}
    end.

shutdown_client_post(#state{asks=Asks, bids=Bids, cancels=Cancels} = State,
                     [Client], _) ->
    case lists:member(Client, Cancels) of
        true ->
            true;
        false when Asks =:= [] ->
            {Drops, _} = bid_drop(State),
            drops_post(Drops--[Client]);
        false when Bids =:= [] ->
            {Drops, _} = ask_drop(State),
            drops_post(Drops--[Client])
    end.

%% client

client_pid({Pid, _}) ->
    Pid.

client_call({Pid, MRef}, Call) ->
    try gen:call(Pid, MRef, Call, 20) of
        {ok, Response} ->
            Response
    catch
        exit:Reason ->
            {exit, Reason}
    end.

client_init(Broker, async_bid) ->
    MRef = monitor(process, Broker),
    {await, ARef, Broker} = sbroker:async_ask_r(Broker),
    client_init(Broker, MRef, ARef, queued);
client_init(Broker, nb_bid) ->
    MRef = monitor(process, Broker),
    State = sbroker:nb_ask_r(Broker),
    client_init(MRef, Broker, undefined, State);
client_init(Broker, async_ask) ->
    MRef = monitor(process, Broker),
    {await, ARef, Broker} = sbroker:async_ask(Broker),
    client_init(Broker, MRef, ARef, queued);
client_init(Broker, nb_ask) ->
    MRef = monitor(process, Broker),
    State = sbroker:nb_ask(Broker),
    client_init(MRef, Broker, undefined, State).

client_init(Broker, MRef, ARef, State) ->
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
                ok ->
                    gen:reply(From, ok),
                    client_loop(MRef, Broker, ARef, cancelled, Froms);
                Error ->
                    gen:reply(From, Error),
                    client_loop(MRef, Broker, ARef, State, Froms)
            end;
        {MRef, From, result} when State =:= queued ->
            client_loop(MRef, Broker, ARef, State, [From | Froms]);
        {MRef, From, result} ->
            gen:reply(From, State),
            client_loop(MRef, Broker, ARef, State, Froms)
    end.
