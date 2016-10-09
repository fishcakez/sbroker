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
-module(sregulator_rate_statem).

-define(TIMEOUT, 500).

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
-export([spawn_client/1]).
-export([client_init/1]).
-export([continue/1]).
-export([done/1]).
-export([cancel/1]).
-export([down/1]).

-record(state, {regulator, target, limit, interval, min, max, queue, clients=[],
                sleep=false, start}).

quickcheck() ->
    quickcheck([]).

quickcheck(Opts) ->
    proper:quickcheck(prop_sregulator_rate(), Opts).

check(CounterExample) ->
    check(CounterExample, []).

check(CounterExample, Opts) ->
    proper:check(prop_sregulator_rate(), CounterExample, Opts).

prop_sregulator_rate() ->
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

command(#state{regulator=undefined} = State) ->
    {call, ?MODULE, start_link, start_link_args(State)};
command(State) ->
    frequency([{8, {call, ?MODULE, spawn_client, spawn_client_args(State)}},
               {8, {call, timer, sleep, sleep_args(State)}},
               {4, {call, ?MODULE, continue, continue_args(State)}},
               {3, {call, ?MODULE, done, done_args(State)}},
               {3, {call, ?MODULE, cancel, cancel_args(State)}},
               {2, {call, ?MODULE, down, down_args(State)}}]).

precondition(State, {call, _, start_link, Args}) ->
    start_link_pre(State, Args);
precondition(#state{regulator=undefined}, _) ->
    false;
precondition(State, {call, _, sleep, Args}) ->
    sleep_pre(State, Args);
precondition(State, {call, _, continue, Args}) ->
    continue_pre(State, Args);
precondition(State, {call, _, done, Args}) ->
    done_pre(State, Args);
precondition(State, {call, _, cancel, Args}) ->
    cancel_pre(State, Args);
precondition(State, {call, _, down, Args}) ->
    down_pre(State, Args);
precondition(_State, _Call) ->
    true.

next_state(#state{sleep=true} = State, Value, Call) ->
    next_state(State#state{sleep=false}, Value, Call);
next_state(State, Value, {call, _, start_link, Args}) ->
    start_link_next(State, Value, Args);
next_state(State, Value, {call, _, spawn_client, Args}) ->
    spawn_client_next(State, Value, Args);
next_state(State, Value, {call, _, sleep, Args}) ->
    sleep_next(State, Value, Args);
next_state(State, Value, {call, _, continue, Args}) ->
    continue_next(State, Value, Args);
next_state(State, Value, {call, _, done, Args}) ->
    done_next(State, Value, Args);
next_state(State, Value, {call, _, cancel, Args}) ->
    cancel_next(State, Value, Args);
next_state(State, Value, {call, _, down, Args}) ->
    down_next(State, Value, Args);
next_state(State, _Value, _Call) ->
    State.

postcondition(State, {call, _, start_link, Args}, Result) ->
    start_link_post(State, Args, Result);
postcondition(State, _, _) ->
    post(State).

cleanup(#state{regulator=undefined}) ->
    ok;
cleanup(#state{regulator=Regulator}) ->
    Trap = process_flag(trap_exit, true),
    exit(Regulator, shutdown),
    receive
        {'EXIT', Regulator, shutdown} ->
            _ = process_flag(trap_exit, Trap),
            ok
    after
        3000 ->
            exit(Regulator, kill),
            _ = process_flag(trap_exit, Trap),
            exit(timeout)
    end.

start_link(Arg) ->
    Trap = process_flag(trap_exit, true),
    case sregulator:start_link(?MODULE, Arg, []) of
        {error, Reason} = Error ->
            receive
                {'EXIT', _, Reason} ->
                    process_flag(trap_exit, Trap),
                    Error
            after
                100 ->
                    exit({timeout, Error})
            end;
        {ok, Pid} ->
            process_flag(trap_exit, Trap),
            {ok, Pid, erlang:monotonic_time(milli_seconds)}
    end.

init({QueueInfo, {Limit, Interval, Min, Max}}) ->
    {QueueSpec, Meters} = queue(QueueInfo),
    ValveSpec = {sregulator_rate_valve, #{limit => Limit,
                                          interval => Interval,
                                          min => Min,
                                          max => Max}},
    {ok, {QueueSpec, ValveSpec, Meters}}.

queue({pie, Target}) ->
    Interval = Target * 10,
    {{sbroker_timeout_queue, #{timeout => Interval}},
     [{sprotector_pie_meter, #{target => Target, interval => Interval}}]};
queue({codel, Target}) ->
    {{sbroker_codel_queue, #{target => Target, interval => Target * 10}}, []};
queue({timeout, Target}) ->
    {{sbroker_timeout_queue, #{timeout => Target}}, []}.

spec() ->
    {queue_spec(), valve_spec()}.

queue_spec() ->
    {oneof([pie]), choose(10, 30)}.

valve_spec() ->
    {choose(0, 5), oneof([50, 100, 150]),
     frequency([{5, 0}, {1, choose(1, 2)}]), oneof([choose(2, 5), infinity])}.

start_link_args(_) ->
    [spec()].

start_link_pre(#state{regulator=Regulator}, _) ->
    Regulator =:= undefined.

start_link_next(_, Value, [{{Queue, Target}, {Limit, Interval, Min, Max}}]) ->
    Regulator = {call, erlang, element, [2, Value]},
    Start = {call, erlang, element, [3, Value]},
    #state{target=Target, limit=Limit, interval=Interval, min=Min, max=Max,
           queue=Queue, regulator=Regulator, start=Start}.

start_link_post(_, _, {ok, Regulator, _}) when is_pid(Regulator) ->
    true;
start_link_post(_, _, _) ->
    false.

spawn_client(Regulator) ->
    {ok, Pid, MRef} = proc_lib:start(?MODULE, client_init, [Regulator]),
    {Pid, MRef}.

spawn_client_args(#state{queue=pie, regulator=Regulator}) ->
    [{via, sprotector, {Regulator, ask}}];
spawn_client_args(#state{regulator=Regulator}) ->
    [Regulator].

spawn_client_next(#state{clients=Clients} = State, Value, _) ->
    State#state{clients=Clients ++ [Value]}.

sleep_args(#state{target=Target}) ->
    [choose(min(Target, 10), Target * 10)].

sleep_pre(#state{sleep=Sleep}, _) ->
    not Sleep.

sleep_next(State, _, _) ->
    State#state{sleep=true}.

continue(Client) ->
    call(Client, continue).

continue_args(#state{clients=[]})      -> [undefined];
continue_args(#state{clients=Clients}) -> [elements(Clients)].

continue_pre(#state{clients=Clients}, [Client]) ->
    lists:member(Client, Clients).

continue_next(State, _, _) ->
    State.

done(Client) ->
    call(Client, done).

done_args(State) ->
    continue_args(State).

done_pre(State, Args) ->
    continue_pre(State, Args).

done_next(State, _, _) ->
    State.

cancel(Client) ->
    call(Client, cancel).

cancel_args(State) ->
    continue_args(State).

cancel_pre(State, Args) ->
    continue_pre(State, Args).

cancel_next(State, _, _) ->
    State.

down(Client) ->
    Result = call(Client, down),
    timer:sleep(10),
    Result.

down_args(State) ->
    continue_args(State).

down_pre(State, Args) ->
    continue_pre(State, Args).

down_next(State, _, _) ->
    State.

post(State) ->
    case do_post(State, false) of
        true ->
            true;
        false ->
            timer:sleep(10),
            do_post(State, true)
    end.

do_post(#state{clients=Clients, limit=Limit, interval=Interval, min=Min,
               max=Max, start=Start}, Retry) ->
    Now = erlang:monotonic_time(milli_seconds),
    States = [call(Client, state) || Client <- Clients],
    Go = length([go || {go, _} <- States]),
    Await = length([await || {await, _} <- States]),
    Done = [Done || {_, Dones} <- States, Done <- Dones],
    RecentDone = length([recent || Time <- Done, Now - Time < Interval]),
    Drops = length([drop || {drop, _} <- States]),

    RealMax = min(Min + Limit, Max),
    if
        Go > RealMax ->
            Retry andalso ct:pal("Too many go: ~p", [Go]),
            false;
        Go > Min, Start + Interval > Now ->
            Retry andalso ct:pal("Too many go: ~p", [Go]),
            false;
        Await > 0, Go < Min ->
            Retry andalso ct:pal("Too many await: ~p", [Await]),
            false;
        Await > 0, Go < Max, RecentDone + Go < Min + Limit,
        Start + Interval < Now ->
            Retry andalso ct:pal("Too many await: ~p", [Await]),
            false;
        Min == 0, RecentDone > Limit ->
            Retry andalso ct:pal("Too many recent done: ~p", [RecentDone]),
            false;
        Drops > 0, length(Done) + Go < Min ->
            Retry andalso ct:pal("Too many drops: ~p", [Drops]),
            false;
        true ->
            true
    end.

%% Internal

call({Pid, MRef} = Client, Req) ->
    try gen:call(Pid, MRef, Req, ?TIMEOUT) of
        {ok, Reply} -> Reply
    catch
        exit:Reason -> error(Reason, [Client, Req])
    end.

client_init(Regulator) when is_pid(Regulator) ->
    {await, MRef, Regulator} = sregulator:async_ask(Regulator),
    proc_lib:init_ack({ok, self(), MRef}),
    await(MRef, Regulator);
client_init({via, _, {Regulator, _}} = Via) ->
    case sregulator:async_ask(Via) of
        {await, MRef, Regulator} ->
            proc_lib:init_ack({ok, self(), MRef}),
            await(MRef, Regulator);
        {drop, _} ->
            MRef = monitor(process, Regulator),
            proc_lib:init_ack({ok, self(), MRef}),
            drop(MRef, Regulator)
    end.

await(MRef, Regulator) ->
    receive
        {'DOWN', MRef, _, _, _} ->
            exit(normal);
        {MRef, {go, VRef, Regulator, _, _}} ->
            go(MRef, Regulator, VRef, []);
        {MRef, {drop, _}} ->
            drop(MRef, Regulator);
        {MRef, From, Fun} when Fun == continue; Fun == done ->
            {not_found, _} = sregulator:Fun(Regulator, MRef),
            gen:reply(From, not_found),
            await(MRef, Regulator);
        {MRef, From, cancel} ->
            case sregulator:cancel(Regulator, MRef) of
                1 ->
                    gen:reply(From, 1),
                    cancel(MRef, Regulator);
                false ->
                    case sregulator:await(MRef, 0) of
                        {go, VRef, Regulator, _, _} ->
                            gen:reply(From, false),
                            go(MRef, Regulator, VRef, []);
                        {drop, _} ->
                            gen:reply(From, false),
                            drop(MRef, Regulator)
                    end
            end;
        {MRef, From, down} ->
            Regulator ! {'DOWN', MRef, process, self(), await},
            gen:reply(From, await),
            await(MRef, Regulator);
        {MRef, From, state} ->
            gen:reply(From, {await, []}),
            await(MRef, Regulator);
        {MRef, Other} ->
            error({badmsg, Other}, [MRef, Regulator])
    end.

go(MRef, Regulator, VRef, Dones) ->
    receive
        {'DOWN', MRef, _, _, _} ->
            exit(normal);
        {MRef, From, continue} ->
            Done = erlang:monotonic_time(milli_seconds),
            case sregulator:continue(Regulator, VRef) of
                {go, VRef, Regulator, _, _} ->
                    gen:reply(From, go),
                    go(MRef, Regulator, VRef, [Done | Dones]);
                {stop, _} ->
                    gen:reply(From, stop),
                    done(MRef, Regulator, [Done | Dones])
            end;
        {MRef, From, done} ->
            Done = erlang:monotonic_time(milli_seconds),
            {stop, _} = sregulator:done(Regulator, VRef),
            gen:reply(From, stop),
            done(MRef, Regulator, [Done | Dones]);
        {MRef, From, cancel} ->
            false = sregulator:cancel(Regulator, MRef),
            gen:reply(From, false),
            go(MRef, Regulator, VRef, Dones);
        {MRef, From, down} ->
            Done = erlang:monotonic_time(milli_seconds),
            Regulator ! {'DOWN', VRef, process, self(), go},
            gen:reply(From, done),
            done(MRef, Regulator, [Done | Dones]);
        {MRef, From, state} ->
            gen:reply(From, {go, Dones}),
            go(MRef, Regulator, VRef, Dones)
    end.

drop(MRef, Regulator) ->
    receive
        {'DOWN', MRef, _, _, _} ->
            exit(normal);
        {MRef, From, Fun} when Fun == continue; Fun == done ->
            {not_found, _} = sregulator:Fun(Regulator, MRef),
            gen:reply(From, not_found),
            drop(MRef, Regulator);
        {MRef, From, cancel} ->
            false = sregulator:cancel(Regulator, MRef),
            gen:reply(From, false),
            drop(MRef, Regulator);
        {MRef, From, down} ->
            Regulator ! {'DOWN', MRef, process, self(), drop},
            gen:reply(From, drop),
            drop(MRef, Regulator);
        {MRef, From, state} ->
            gen:reply(From, {drop, []}),
            drop(MRef, Regulator)
    end.

cancel(MRef, Regulator) ->
    receive
        {'DOWN', MRef, _, _, _} ->
            exit(normal);
        {MRef, From, Fun} when Fun == continue; Fun == done ->
            {not_found, _} = sregulator:Fun(Regulator, MRef),
            gen:reply(From, not_found),
            cancel(MRef, Regulator);
        {MRef, From, cancel} ->
            false = sregulator:cancel(Regulator, MRef),
            gen:reply(From, false),
            cancel(MRef, Regulator);
        {MRef, From, down} ->
            Regulator ! {'DOWN', MRef, process, self(), cancel},
            gen:reply(From, cancel),
            cancel(MRef, Regulator);
        {MRef, From, state} ->
            gen:reply(From, {cancel, []}),
            cancel(MRef, Regulator)
    end.

done(MRef, Regulator, Dones) ->
    receive
        {'DOWN', MRef, _, _, _} ->
            exit(normal);
        {MRef, From, Fun} when Fun == continue; Fun == done ->
            {not_found, _} = sregulator:Fun(Regulator, MRef),
            gen:reply(From, not_found),
            done(MRef, Regulator, Dones);
        {MRef, From, cancel} ->
            false = sregulator:cancel(Regulator, MRef),
            gen:reply(From, false),
            done(MRef, Regulator, Dones);
        {MRef, From, down} ->
            Regulator ! {'DOWN', MRef, process, self(), done},
            gen:reply(From, done),
            done(MRef, Regulator, Dones);
        {MRef, From, state} ->
            gen:reply(From, {done, Dones}),
            done(MRef, Regulator, Dones)
    end.
