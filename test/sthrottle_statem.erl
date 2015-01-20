-module(sthrottle_statem).

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
-export([signal/2]).
-export([done/1]).
-export([cancel/1]).
-export([shutdown_client/1]).

-export([client_init/2]).

-record(state, {sthrottle, asks=[], out, drops, q_state=[], q_size, drop,
                active=[], done=[], cancels=[], min, max, size,
                replies=orddict:new()}).

quickcheck() ->
    quickcheck([]).

quickcheck(Opts) ->
    proper:quickcheck(prop_sthrottle(), Opts).

check(CounterExample) ->
    check(CounterExample, []).

check(CounterExample, Opts) ->
    proper:check(prop_sthrottle(), CounterExample, Opts).

prop_sthrottle() ->
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

command(#state{sthrottle=undefined}) ->
    {call, sthrottle, start_link, start_link_args()};
command(State) ->
    frequency([{15, {call, ?MODULE, signal, signal_args(State)}} ||
               signal_command(State)] ++
              [{5, {call, ?MODULE, spawn_client, spawn_client_args(State)}}] ++
              [{5, {call, ?MODULE, done, done_args(State)}} ||
               done_command(State)] ++
              [{4, {call, ?MODULE, cancel, cancel_args(State)}} ||
               cancel_command(State)] ++
              [{2, {call, ?MODULE, shutdown_client,
                    shutdown_client_args(State)}} ||
                    shutdown_client_command(State)] ++
              [{2, {call, sthrottle, positive, positive_args(State)}},
               {2, {call, sthrottle, negative, negative_args(State)}},
               {2, {call, sthrottle, force_timeout,
                    force_timeout_args(State)}}]).

precondition(State, {call, _, start_link, Args}) ->
    start_link_pre(State, Args);
precondition(#state{sthrottle=undefined}, _) ->
    false;
precondition(State, {call, _, signal, Args}) ->
    signal_pre(State, Args);
precondition(State, {call, _, done, Args}) ->
    done_pre(State, Args);
precondition(State, {call, _, cancel, Args}) ->
    cancel_pre(State, Args);
precondition(State, {call, _, shutdown_client, Args}) ->
    shutdown_client_pre(State, Args);
precondition(_, _) ->
    true.

next_state(State, Value, {call, _, start_link, Args}) ->
    start_link_next(State, Value, Args);
next_state(State, Value, {call, _, signal, Args}) ->
    signal_next(State, Value, Args);
next_state(State, Value, {call, _, spawn_client, Args}) ->
    spawn_client_next(State, Value, Args);
next_state(State, Value, {call, _, done, Args}) ->
    done_next(State, Value, Args);
next_state(State, Value, {call, _, cancel, Args}) ->
    cancel_next(State, Value, Args);
next_state(State, Value, {call, _, shutdown_client, Args}) ->
    shutdown_client_next(State, Value, Args);
next_state(State, Value, {call, _, positive, Args}) ->
    positive_next(State, Value, Args);
next_state(State, Value, {call, _, negative, Args}) ->
    negative_next(State, Value, Args);
next_state(State, Value, {call, _, force_timeout, Args}) ->
    force_timeout_next(State, Value, Args);
next_state(State, _, _) ->
    State.

postcondition(State, {call, _, start_link, Args}, Result) ->
    start_link_post(State, Args, Result);
postcondition(State, {call, _, signal, Args}, Result) ->
    signal_post(State, Args, Result);
postcondition(State, {call, _, spawn_client, Args}, Result) ->
    spawn_client_post(State, Args, Result);
postcondition(State, {call, _, done, Args}, Result) ->
    done_post(State, Args, Result);
postcondition(State, {call, _, cancel, Args}, Result) ->
    cancel_post(State, Args, Result);
postcondition(State, {call, _, shutdown_client, Args}, Result) ->
    shutdown_client_post(State, Args, Result);
postcondition(State, {call, _, positive, Args}, Result) ->
    positive_post(State, Args, Result);
postcondition(State, {call, _, negative, Args}, Result) ->
    negative_post(State, Args, Result);
postcondition(State, {call, _, force_timeout, Args}, Result) ->
    force_timeout_post(State, Args, Result);
postcondition(_, _, _) ->
    true.

cleanup(#state{sthrottle=undefined}) ->
    ok;
cleanup(#state{sthrottle=Throttle}) ->
    Trap = process_flag(trap_exit, true),
    exit(Throttle, shutdown),
    receive
        {'EXIT', Throttle, shutdown} ->
            _ = process_flag(trap_exit, Trap),
            ok
    after
        3000 ->
            exit(Throttle, kill),
            _ = process_flag(trap_exit, Trap),
            exit(timeout)
    end.

start_link_args() ->
    ?LET(Min, choose(0,5),
         ?LET(Max, oneof([choose(Min, 5), infinity]),
              [Min, Max, queue_spec(), 10000])).

queue_spec() ->
    {sbroker_statem_queue, list(oneof([0, choose(1, 2)])), oneof([out, out_r]),
     oneof([choose(0, 5), infinity]), oneof([drop, drop_r])}.

start_link_pre(#state{sthrottle=Throttle}, [Min, Max, _, _]) ->
    Throttle =:= undefined andalso Min < Max.

start_link_next(State, Value, [Min, Max,
                               {_, Drops, Out, QSize, Drop}, _]) ->
    Throttle = {call, erlang, element, [2, Value]},
    State#state{sthrottle=Throttle, out=Out, drops=Drops, q_state=Drops,
                q_size=QSize, drop=Drop, min=Min, max=Max, size=Min}.

start_link_post(_, _, {ok, Throttle}) when is_pid(Throttle) ->
    true;
start_link_post(_, _, _) ->
    false.

signal_command(#state{active=Active}) ->
    Active =/= [].

signal_args(#state{active=Active}) ->
    [elements(Active),
     oneof([{go, make_ref(), self(), oneof([0, choose(1, 5)])},
            {drop, choose(1, 5)}, {retry, choose(0, 5)}])].

signal(Client, Reply) ->
    client_call(Client, {signal, Reply}).

signal_pre(#state{active=Active}, [Client, _]) ->
    lists:member(Client, Active).

signal_next(#state{sthrottle=Throttle, replies=Replies} = State, _,
                  [Client, {go, _, _, 0} = Reply]) ->
    NReplies = orddict:store(Client, Reply, Replies),
    NState = State#state{replies=NReplies},
    case orddict:find(Client, Replies) of
        {ok, {go, _, _, 0}} ->
            positive_next(NState, ok, [Throttle]);
        _ ->
            NState
    end;
signal_next(#state{replies=Replies} = State, _,
                  [Client, {go, _, _, _} = Reply]) ->
    NReplies = orddict:store(Client, Reply, Replies),
    State#state{replies=NReplies};
signal_next(#state{active=Active, min=Min, replies=Replies, done=Done,
                         size=Size} = State, _, [Client, {drop, _} = Reply])
  when length(Active) > Min ->
    NActive = Active--[Client],
    NReplies = orddict:store(Client, Reply, Replies),
    State#state{active=NActive, done=Done++[Client], replies=NReplies,
                size=min(length(NActive), Size)};
signal_next(#state{replies=Replies, min=Min} = State, _,
                  [Client, {drop, _} = Reply]) ->
    NReplies = orddict:store(Client, Reply, Replies),
    State#state{replies=NReplies, size=Min};
signal_next(#state{replies=Replies} = State, _,
                  [Client, {retry, _} = Reply]) ->
    NReplies = orddict:store(Client, Reply, Replies),
    State#state{replies=NReplies}.

signal_post(#state{sthrottle=Throttle, replies=Replies} = State,
                  [Client, {go, _, _, 0} = Reply], Result) ->
    case orddict:find(Client, Replies) of
        {ok, {go, _, _, 0}} ->
            Result =:= Reply andalso positive_post(State, [Throttle], ok);
        _ ->
            Result =:= Reply
    end;
signal_post(_, [_, {go, _, _, _} = Reply], Result) ->
    Result =:= Reply;
signal_post(#state{active=Active, min=Min}, [_, {drop, SojournTime}],
                  Result) when length(Active) > Min ->
    Result =:= {done, SojournTime};
signal_post(_, [_, {drop, _} = Reply], Result) ->
    Result =:= Reply;
signal_post(_, [_, {retry, _} = Reply], Result) ->
    Result =:= Reply.

spawn_client_args(#state{sthrottle=Throttle}) ->
    [Throttle, oneof([ask, nb_ask])].

spawn_client(Throttle, AskFun) ->
    {ok, Pid, MRef} = proc_lib:start(?MODULE, client_init, [Throttle, AskFun]),
    {Pid, MRef}.

spawn_client_next(#state{active=Active, size=Size} = State, Client, [_, _])
  when length(Active) < Size ->
    State#state{active=Active ++ [Client]};
spawn_client_next(#state{q_size=QSize, drop=Drop} = State, Client, [_, ask]) ->
    #state{asks=Asks} = NState = ask_drop_state(State),
    NState2 = NState#state{asks=Asks ++ [Client]},
    case length(Asks) + 1 > QSize of
        true when Drop =:= drop ->
            #state{asks=NAsks} = NState3 = ask_drop_state(NState2),
            NState3#state{asks=dropfirst(NAsks)};
        true when Drop =:= drop_r ->
            #state{asks=NAsks} = NState3 = ask_drop_state(NState2),
            NState3#state{asks=droplast(NAsks)};
        false ->
            NState2
    end;
spawn_client_next(State, _, [_, nb_ask]) ->
    State.

spawn_client_post(#state{active=Active, size=Size}, [_, _], Client)
  when length(Active) < Size ->
    go_post(Client);
spawn_client_post(#state{q_size=QSize, drop=Drop} = State, [_, ask], Client) ->
    {Drops, #state{asks=Asks} = NState} = ask_drop(State),
    NState2 = NState#state{asks=Asks ++ [Client]},
    case length(Asks) + 1 > QSize of
        true when Drop =:= drop ->
            {Drops2, #state{asks=NAsks}} = ask_drop(NState2),
            drops_post(Drops) andalso drops_post(Drops2) andalso
            (NAsks =:= [] orelse drops_post([hd(NAsks)]));
        true when Drop =:= drop_r ->
            {Drops2, #state{asks=NAsks}}= ask_drop(NState2),
            drops_post(Drops) andalso drops_post(Drops2) andalso
            (NAsks =:= [] orelse drops_post([lists:last(NAsks)]));
        false ->
            drops_post(Drops)
    end;
spawn_client_post(_, [_, nb_ask], Client) ->
    retry_post(Client).

done(Client) ->
    client_call(Client, done).

done_command(#state{active=Active}) ->
    Active =/= [].

done_args(#state{active=Active}) ->
    [elements(Active)].

done_pre(#state{active=Active}, [Client]) ->
    lists:member(Client, Active).

done_next(#state{active=Active, size=Size, done=Done} = State, _, [Client])
  when length(Active) > Size ->
    State#state{active=Active--[Client], done=Done++[Client]};
done_next(#state{active=Active, done=Done, out=Out} = State, _, [Client]) ->
    NState = State#state{active=Active--[Client], done=Done++[Client]},
    case ask_drop_state(NState) of
        #state{asks=[]} = NState2 ->
            NState2;
        #state{asks=Asks, active=NActive} = NState2 when Out =:= out ->
            NState2#state{active=NActive++[hd(Asks)], asks=dropfirst(Asks)};
        #state{asks=Asks, active=NActive} = NState2 when Out =:= out_r ->
            NState2#state{active=NActive++[lists:last(Asks)],
                          asks=droplast(Asks)}
    end.

done_post(#state{active=Active, size=Size}, _, ok) when length(Active) > Size ->
    true;
done_post(#state{out=Out} = State, _, ok) ->
    case ask_drop(State) of
        {Drops, #state{asks=[]}} ->
            drops_post(Drops);
        {Drops, #state{asks=Asks}} when Out =:= out ->
            go_post(hd(Asks)) andalso drops_post(Drops);
        {Drops, #state{asks=Asks}} when Out =:= out_r ->
            go_post(lists:last(Asks)) andalso drops_post(Drops)
    end;
done_post(_, _, {error, _}) ->
    false.


cancel_command(#state{asks=[]}) ->
    false;
cancel_command(_) ->
    true.

cancel_args(#state{asks=Asks}) ->
    [elements(Asks)].

cancel(Client) ->
    client_call(Client, cancel).

cancel_pre(#state{asks=Asks}, [Client]) ->
    lists:member(Client, Asks).

cancel_next(#state{cancels=Cancels} = State, _, [Client]) ->
    NState = #state{asks=Asks} = ask_drop_state(State),
    NState#state{asks=Asks--[Client], cancels=Cancels++[Client]}.

cancel_post(State, _, ok) ->
    {Drops, _} = ask_drop(State),
    drops_post(Drops);
cancel_post(State, [Client], {error, not_found}) ->
    {Drops, _} = ask_drop(State),
    drops_post(Drops) andalso lists:member(Client, Drops).

shutdown_client_command(#state{asks=[], active=[], done=[], cancels=[]}) ->
    false;
shutdown_client_command(_) ->
    true.

shutdown_client_args(#state{asks=Asks, active=Active, done=Done,
                            cancels=Cancels}) ->
    [elements(Asks ++ Active ++ Done ++ Cancels)].

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

shutdown_client_pre(#state{asks=Asks, active=Active, done=Done,
                           cancels=Cancels},
                    [Client]) ->
    lists:member(Client, Asks) orelse lists:member(Client, Active) orelse
    lists:member(Client, Done) orelse lists:member(Client, Cancels).

shutdown_client_next(#state{asks=Asks, active=Active, done=Done,
                            cancels=Cancels, size=Size, out=Out} = State, _,
                     [Client]) ->
    case {lists:member(Client, Asks), lists:member(Client, Active)} of
        {true, false} ->
            NState = #state{asks=NAsks} = ask_drop_state(State),
            NState#state{asks=NAsks--[Client]};
        {false, true} when length(Active) > Size ->
            State#state{active=Active--[Client]};
        {false, true} ->
            NState = State#state{active=Active--[Client]},
            case ask_drop_state(NState) of
                #state{asks=[]} = NState2 ->
                    NState2;
                #state{asks=NAsks, active=NActive} = NState2 when Out =:= out ->
                    NState2#state{active=NActive++[hd(NAsks)],
                                  asks=dropfirst(NAsks)};
                #state{asks=NAsks, active=NActive} = NState2
                  when Out =:= out_r ->
                    NState2#state{active=NActive++[lists:last(NAsks)],
                                  asks=droplast(NAsks)}
            end;
        {false, false} ->
            State#state{done=Done--[Client], cancels=Cancels--[Client]}
    end.

shutdown_client_post(#state{asks=Asks, active=Active, size=Size,
                            out=Out} = State, [Client], _) ->
    case {lists:member(Client, Asks), lists:member(Client, Active)} of
        {true, false} ->
            {Drops, _} = ask_drop(State),
            drops_post(Drops--[Client]);
        {false, true} when length(Active) > Size ->
            true;
        {false, true} ->
            NState = State#state{active=Active--[Client]},
            case ask_drop(NState) of
                {Drops, #state{asks=[]}} ->
                    drops_post(Drops);
                {Drops, #state{asks=NAsks}} when Out =:= out ->
                    drops_post(Drops) andalso go_post(hd(NAsks));
                {Drops, #state{asks=NAsks}} when Out =:= out_r ->
                    drops_post(Drops) andalso go_post(lists:last(NAsks))
            end;
        {false, false} ->
            true
    end.

positive_args(#state{sthrottle=Throttle}) ->
    [Throttle].

positive_next(#state{size=Max, max=Max} = State, _, _) ->
    State;
positive_next(#state{active=Active, size=Size} = State, _, _)
  when length(Active) > Size ->
    State#state{size=Size+1};
positive_next(#state{active=Active, out=Out, size=Size} = State, _, _) ->
    NState = State#state{size=Size+1},
    case ask_drop_state(NState) of
        #state{asks=[]} = NState2 ->
            NState2;
        #state{asks=Asks} = NState2 when Out =:= out ->
            NState2#state{active=Active++[hd(Asks)],
                          asks=dropfirst(Asks)};
        #state{asks=Asks} = NState2 when Out =:= out_r ->
            NState2#state{active=Active++[lists:last(Asks)],
                          asks=droplast(Asks)}
    end.

positive_post(#state{size=Max, max=Max}, _, _) ->
    true;
positive_post(#state{active=Active, size=Size}, _, _)
  when length(Active) > Size ->
    true;
positive_post(#state{out=Out, size=Size} = State, _, _) ->
    NState = State#state{size=Size+1},
    case ask_drop(NState) of
        {Drops, #state{asks=[]}} ->
            drops_post(Drops);
        {Drops, #state{asks=Asks}} when Out =:= out ->
            drops_post(Drops) andalso go_post(hd(Asks));
        {Drops, #state{asks=Asks}} when Out =:= out_r ->
            drops_post(Drops) andalso go_post(lists:last(Asks))
    end.

negative_args(#state{sthrottle=Throttle}) ->
    [Throttle].

negative_next(#state{size=Size, min=Min} = State, _, _) ->
    State#state{size=max(Size-1, Min)}.

negative_post(_, _, _) ->
    true.

force_timeout_args(#state{sthrottle=Throttle}) ->
    [Throttle].

force_timeout_next(State, _, _) ->
    ask_drop_state(State).

force_timeout_post(State, _, _) ->
    {Drops, _} = ask_drop(State),
    drops_post(Drops).

%% helpers

droplast([]) ->
    [];
droplast(List) ->
    [_ | Rest] = lists:reverse(List),
    lists:reverse(Rest).

dropfirst([]) ->
    [];
dropfirst([_ | Rest]) ->
    Rest.

ask_drop_state(State) ->
    {_, NState} = ask_drop(State),
    NState.

ask_drop(#state{asks=[]} = State) ->
    {[], State};
ask_drop(#state{q_state=[], drops=[]} = State) ->
    {[], State};
ask_drop(#state{q_state=[], drops=Drops} = State) ->
    ask_drop(State#state{q_state=Drops});
ask_drop(#state{asks=Asks, q_state=[Drop | NQState]} = State) ->
    Drop2 = min(length(Asks), Drop),
    {Drops, NAsks} = lists:split(Drop2, Asks),
    {Drops, State#state{asks=NAsks, q_state=NQState}}.

go_post(Client) ->
    case result(Client) of
        {go, GRef, _, _} when is_reference(GRef) ->
            true;
        _ ->
            false
    end.

retry_post(Client) ->
    case result(Client) of
        {retry, 0} ->
            true;
        _ ->
            false
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

%% client

client_init(Throttle, ask) ->
    MRef = monitor(process, Throttle),
    ARef = sthrottle:async_ask(Throttle),
    proc_lib:init_ack({ok, self(), MRef}),
    client_loop(MRef, Throttle, ARef, queued, []);
client_init(Throttle, nb_ask) ->
    MRef = monitor(process, Throttle),
    State = sthrottle:nb_ask(Throttle),
    proc_lib:init_ack({ok, self(), MRef}),
    client_loop(MRef, Throttle, undefined, State, []).

result(Client) ->
    client_call(Client, result).

client_pid({Pid, _}) ->
    Pid.

client_call({Pid, MRef}, Call) ->
    try gen:call(Pid, MRef, Call, 50) of
        {ok, Response} ->
            Response
    catch
        exit:Reason ->
            {exit, Reason}
    end.

client_loop(MRef, Throttle, ARef, State, Froms) ->
    receive
        {'DOWN', MRef, _, _, _} ->
            exit(normal);
        {ARef, Result} when State =:= queued ->
            _ = [gen:reply(From, Result) || From <- Froms],
            client_loop(MRef, Throttle, ARef, Result, []);
        {ARef, Result} when State =/= queued ->
            exit(Throttle, {double_result, {self(), MRef}, State, Result}),
            exit(normal);
        {MRef, From, {signal, Reply}} ->
            {go, GRef, _, _} = State,
            case sthrottle:signal(Throttle, GRef, Reply) of
                {done, _} = Result ->
                    gen:reply(From, Result),
                    client_loop(MRef, Throttle, ARef, done, Froms);
                Result ->
                    gen:reply(From, Result),
                    client_loop(MRef, Throttle, ARef, State, Froms)
            end;
        {MRef, From, cancel} ->
            case sthrottle:cancel(Throttle, ARef) of
                ok ->
                    gen:reply(From, ok),
                    client_loop(MRef, Throttle, ARef, cancelled, Froms);
                Error ->
                    gen:reply(From, Error),
                    client_loop(MRef, Throttle, ARef, State, Froms)
            end;
        {MRef, From, done} ->
            {go, GRef, _, _} = State,
            case sthrottle:done(Throttle, GRef) of
                ok ->
                    gen:reply(From, ok),
                    client_loop(MRef, Throttle, ARef, done, Froms);
                Error ->
                    gen:reply(From, Error),
                    client_loop(MRef, Throttle, ARef, State, Froms)
            end;
        {MRef, From, result} when State =:= queued ->
            client_loop(MRef, Throttle, ARef, State, [From | Froms]);
        {MRef, From, result} ->
            gen:reply(From, State),
            client_loop(MRef, Throttle, ARef, State, Froms)
    end.
