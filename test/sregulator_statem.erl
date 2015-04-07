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
-module(sregulator_statem).

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
-export([update/3]).
-export([done/2]).
-export([ensure_dropped/2]).
-export([cancel/2]).
-export([shutdown_client/2]).
-export([change_config/2]).

-export([client_init/2]).

-record(state, {sregulator, asks=[], ask_out, ask_drops, ask_state=[], ask_size,
                ask_drop, valve_status, valve_state=[], min=0, max=0, active=[],
                done=[], cancels=[], drops=[]}).

-define(TIMEOUT, 5000).

quickcheck() ->
    quickcheck([]).

quickcheck(Opts) ->
    proper:quickcheck(prop_sregulator(), Opts).

check(CounterExample) ->
    check(CounterExample, []).

check(CounterExample, Opts) ->
    proper:check(prop_sregulator(), CounterExample, Opts).

prop_sregulator() ->
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

command(#state{sregulator=undefined} = State) ->
    {call, ?MODULE, start_link, start_link_args(State)};
command(State) ->
    frequency([{25, {call, ?MODULE, update, update_args(State)}},
               {10, {call, ?MODULE, spawn_client,
                     spawn_client_args(State)}},
               {6, {call, ?MODULE, done, done_args(State)}},
               {4, {call, ?MODULE, ensure_dropped, ensure_dropped_args(State)}},
               {4, {call, ?MODULE, cancel, cancel_args(State)}},
               {4, {call, ?MODULE, shutdown_client,
                    shutdown_client_args(State)}},
               {4, {call, ?MODULE, change_config, change_config_args(State)}},
               {2, {call, sregulator, timeout,
                    timeout_args(State)}}]).

precondition(State, {call, _, start_link, Args}) ->
    start_link_pre(State, Args);
precondition(#state{sregulator=undefined}, _) ->
    false;
precondition(_State, _Call) ->
    true.

next_state(State, Value, {call, _, start_link, Args}) ->
    start_link_next(State, Value, Args);
next_state(State, Value, {call, _, spawn_client, Args}) ->
    spawn_client_next(State, Value, Args);
next_state(State, Value, {call, _, update, Args}) ->
    update_next(State, Value, Args);
next_state(State, Value, {call, _, done, Args}) ->
    done_next(State, Value, Args);
next_state(State, Value, {call, _, ensure_dropped, Args}) ->
    ensure_dropped_next(State, Value, Args);
next_state(State, Value, {call, _, cancel, Args}) ->
    cancel_next(State, Value, Args);
next_state(State, Value, {call, _, shutdown_client, Args}) ->
    shutdown_client_next(State, Value, Args);
next_state(State, Value, {call, _, timeout, Args}) ->
    timeout_next(State, Value, Args);
next_state(State, Value, {call, _, change_config, Args}) ->
    change_config_next(State, Value, Args).

postcondition(State, {call, _, start_link, Args}, Result) ->
    start_link_post(State, Args, Result);
postcondition(State, {call, _, spawn_client, Args}, Result) ->
    spawn_client_post(State, Args, Result);
postcondition(State, {call, _, update, Args}, Result) ->
    update_post(State, Args, Result);
postcondition(State, {call, _, done, Args}, Result) ->
    done_post(State, Args, Result);
postcondition(State, {call, _, ensure_dropped, Args}, Result) ->
    ensure_dropped_post(State, Args, Result);
postcondition(State, {call, _, cancel, Args}, Result) ->
    cancel_post(State, Args, Result);
postcondition(State, {call, _, shutdown_client, Args}, Result) ->
    shutdown_client_post(State, Args, Result);
postcondition(State, {call, _, timeout, Args}, Result) ->
    timeout_post(State, Args, Result);
postcondition(State, {call, _, change_config, Args}, Result) ->
    change_config_post(State, Args, Result).

cleanup(#state{sregulator=undefined}) ->
    application:unset_env(sbroker, ?MODULE);
cleanup(#state{sregulator=Regulator}) ->
    application:unset_env(sbroker, ?MODULE),
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

start_link_args(_) ->
    [init()].

init() ->
    frequency([{10, {ok, {queue_spec(), valve_spec(), 10000}}},
               {1, ignore},
               {1, bad}]).

queue_spec() ->
    {sbroker_statem_queue, resize(4, list(oneof([0, choose(1, 2)]))),
     oneof([out, out_r]), oneof([choose(0, 5), infinity]),
     oneof([drop, drop_r])}.

valve_spec() ->
    ?LET(Min, choose(0,5),
         {sregulator_statem_valve, resize(4, list(oneof([open, closed]))),
          Min, oneof([choose(Min, 5), infinity])}).

start_link(Init) ->
    application:set_env(sbroker, ?MODULE, Init),
    Trap = process_flag(trap_exit, true),
    case sregulator:start_link(?MODULE, []) of
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
    case Result of
        {ok, {AskSpec, ValveSpec, Interval}} ->
            NInterval = sbroker_time:milli_seconds_to_native(Interval),
            {ok, {AskSpec, ValveSpec, NInterval}};
        Other ->
            Other
    end.

start_link_pre(#state{sregulator=Regulator}, _) ->
    Regulator =:= undefined.

start_link_next(State, Value,
                [{ok, {{_, AskDrops, AskOut, AskSize, AskDrop},
                       {_, ValveStatus, Min, Max}, _}}]) ->
    Regulator = {call, erlang, element, [2, Value]},
    State#state{sregulator=Regulator, ask_out=AskOut, ask_drops=AskDrops,
                ask_state=AskDrops, ask_size=AskSize, ask_drop=AskDrop,
                valve_state=ValveStatus, valve_status=ValveStatus, min=Min,
                max=Max};
start_link_next(State, _, [ignore]) ->
    State;
start_link_next(State, _, [bad]) ->
    State.

start_link_post(_, [{ok, _}], {ok, Regulator}) when is_pid(Regulator) ->
    true;
start_link_post(_, [ignore], ignore) ->
    true;
start_link_post(_, [bad], {error, {bad_return, {?MODULE, init, bad}}}) ->
    true;
start_link_post(_, _, _) ->
    false.

spawn_client_args(#state{sregulator=Regulator}) ->
    [Regulator, oneof([async_ask, nb_ask])].

spawn_client(Regulator, Fun) ->
    {ok, Pid, MRef} = proc_lib:start(?MODULE, client_init, [Regulator, Fun]),
    {Pid, MRef}.

spawn_client_next(#state{asks=[_ | _], active=Active, min=Min} = State, Ask, Args)
  when length(Active) < Min ->
    spawn_client_next(dequeue_next(State), Ask, Args);
spawn_client_next(#state{active=Active, min=Min} = State, Ask, [_, _])
  when length(Active) < Min ->
    State#state{active=Active++[Ask]};
spawn_client_next(State, Ask, [_, async_ask]) ->
    ask_next(State,
             fun(#state{asks=NAsks} = NState) ->
                     NState#state{asks=NAsks++[Ask]}
             end);
spawn_client_next(State, _, [_, nb_ask]) ->
    State.

spawn_client_post(#state{asks=[_ | _], active=Active, min=Min} = State, Ask, Args)
  when length(Active) < Min ->
    case dequeue(State) of
        {true, NState} ->
            spawn_client_post(NState, Ask, Args);
        false ->
            false
    end;
spawn_client_post(#state{active=Active, min=Min, sregulator=Regulator}, [_, _],
                  Ask) when length(Active) < Min ->
    go_post(Ask, Regulator);
spawn_client_post(State, [_, async_ask], Ask) ->
    ask_post(State,
             fun(#state{asks=NAsks} = NState) ->
                     {true, NState#state{asks=NAsks++[Ask]}}
             end);
spawn_client_post(_, [_, nb_ask], Ask) ->
    retry_post(Ask).

update() ->
    frequency([{10, {go, choose(0, 5)}},
               {5, {drop, choose(0, 5)}},
               {1, {retry, choose(0, 5)}}]).

update_args(#state{sregulator=Regulator, asks=Asks, active=Active, done=Done,
                   cancels=Cancels}) ->
    case Asks ++ Active ++ Done ++ Cancels of
        [] ->
            [Regulator, undefined, update()];
        Clients ->
            [Regulator, frequency([{9, elements(Clients)},
                                   {1, undefined}]), update()]
    end.

update(Regulator, Ref, {go, SojournTime}) ->
    update(Regulator, Ref, {go, make_ref(), self(), SojournTime});
update(Regulator, undefined, Response) ->
    sregulator:update(Regulator, make_ref(), Response);
update(_, Client, Response) ->
    client_call(Client, {update, Response}).

update_next(State, _, [_, _, {go, _}]) ->
    update_next(State);
update_next(State, _, [_, _, {retry, _}]) ->
    State;
update_next(#state{active=Active, min=Min} = State, _,
            [_, Client, {drop, _}]) ->
    case lists:member(Client, Active) of
        true when length(Active) =< Min ->
            update_next(State);
        true ->
            update_next(State#state{active=Active--[Client]});
        false ->
            dequeue_next(State)
    end.

update_next(#state{asks=[_ | _], active=Active, min=Min} = State)
  when length(Active) < Min ->
    update_next(dequeue_next(State));
update_next(State) ->
    case valve_status(State) of
        {closed, NState} ->
            ask_next(NState, fun(NState2) -> NState2 end);
        {open, NState} ->
            ask_out_next(NState)
    end.

update_post(State, [_, _, {go, SojournTime}], Result) ->
    case Result of
        {go, _, _, SojournTime} ->
            update_post(State);
        _ ->
            false
    end;
update_post(_, [_, _, {retry, _} = Response], Result) ->
    Response =:= Result;
update_post(State, [_, undefined, {drop, SojournTime}], Result) ->
    case Result of
        {not_found, NSojournTime} ->
            is_integer(NSojournTime) andalso NSojournTime >= SojournTime andalso
            dequeue_post(State);
        _ ->
            false
    end;
update_post(#state{active=Active, min=Min} = State,
            [_, Client, {drop, SojournTime}], Result) ->
    case Result of
        {_, NSojournTime}
          when not (is_integer(NSojournTime) andalso
                    NSojournTime >= SojournTime) ->
            false;
        {retry, _} ->
            lists:member(Client, Active) andalso length(Active) =< Min andalso
            update_post(State);
        {dropped, _} ->
            lists:member(Client, Active) andalso length(Active) > Min andalso
            update_post(State);
        {not_found, _} ->
            (not lists:member(Client, Active)) andalso dequeue_post(State);
        _ ->
            false
    end.

update_post(#state{asks=[_ | _], active=Active, min=Min} = State)
  when length(Active) < Min ->
    case dequeue(State) of
        {true, NState} ->
            update_post(NState);
        false ->
            false
    end;
update_post(State) ->
    case valve_status(State) of
        {closed, NState} ->
            ask_post(NState, fun(NState2) -> {true, NState2} end);
        {open, NState} ->
            ask_out_post(NState)
    end.

done_args(#state{sregulator=Regulator, asks=Asks, active=Active, done=Done,
                 cancels=Cancels}) ->
    case Asks ++ Active ++ Done ++ Cancels of
        [] ->
            [Regulator, undefined];
        Clients ->
            [Regulator, frequency([{9, elements(Clients)},
                                   {1, undefined}])]
    end.

done(Regulator, undefined) ->
    sregulator:done(Regulator, make_ref(), ?TIMEOUT);
done(_, Client) ->
    client_call(Client, done).

done_next(#state{active=Active, done=Done} = State, _, [_, Client]) ->
    case lists:member(Client, Active) of
        true ->
            NState = State#state{active=Active--[Client], done=Done++[Client]},
            ask_out_next(NState);
        false ->
            dequeue_next(State)
    end.

done_post(#state{active=Active, done=Done} = State, [_, Client], Result) ->
    case lists:member(Client, Active) of
        true ->
            NState = State#state{active=Active--[Client], done=Done++[Client]},
            Result =:= ok andalso ask_out_post(NState);
        false ->
            Result =:= {error, not_found} andalso dequeue_post(State)
    end.

ensure_dropped_args(#state{sregulator=Regulator, asks=Asks, active=Active,
                           done=Done, cancels=Cancels}) ->
    case Asks ++ Active ++ Done ++ Cancels of
        [] ->
            [Regulator, undefined];
        Clients ->
            [Regulator, frequency([{9, elements(Clients)},
                                   {1, undefined}])]
    end.

ensure_dropped(Regulator, undefined) ->
    sregulator:ensure_dropped(Regulator, make_ref(), ?TIMEOUT);
ensure_dropped(_, Client) ->
    client_call(Client, ensure_dropped).

ensure_dropped_next(#state{active=Active} = State, _, [_, Client]) ->
    case lists:member(Client, Active) of
        true ->
            update_next(State#state{active=Active--[Client]});
        false ->
            dequeue_next(State)
    end.

ensure_dropped_post(#state{active=Active} = State, [_, Client], Result) ->
    case lists:member(Client, Active) of
        true ->
            NState = State#state{active=Active--[Client]},
            Result =:= ok andalso update_post(NState);
        false ->
            Result =:= {error, not_found} andalso dequeue_post(State)
    end.

cancel_args(#state{sregulator=Regulator, asks=Asks, active=Active, done=Done,
                   cancels=Cancels, drops=Drops}) ->
    case Asks ++ Active ++ Done ++ Cancels ++ Drops of
        [] ->
            [Regulator, undefined];
        Clients ->
            [Regulator, frequency([{9, elements(Clients)},
                                   {1, undefined}])]
    end.

cancel(Regulator, undefined) ->
    sregulator:cancel(Regulator, make_ref(), ?TIMEOUT);
cancel(_, Client) ->
    client_call(Client, cancel).

cancel_next(State, _, [_, Client]) ->
    ask_next(State,
             fun(#state{asks=Asks, cancels=Cancels} = NState) ->
                     case lists:member(Client, Asks) andalso
                          not lists:member(Client, Cancels) of
                         true ->
                             NState#state{asks=Asks--[Client],
                                          cancels=Cancels++[Client]};
                         false ->
                             NState
                     end
             end).

cancel_post(State, [_, Client], Result) ->
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

shutdown_client_args(#state{sregulator=Regulator, asks=Asks, active=Active,
                            done=Done, cancels=Cancels, drops=Drops}) ->
    case Asks ++ Active ++ Done ++ Cancels ++ Drops of
        [] ->
            [Regulator, undefined];
        Clients ->
            [Regulator, frequency([{9, elements(Clients)},
                                   {1, undefined}])]
    end.

shutdown_client(Regulator, undefined) ->
    _ = Regulator ! {'DOWN', make_ref(), process, self(), shutdown},
    ok;
shutdown_client(_, Client) ->
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

shutdown_client_next(State, _, [_, undefined]) ->
    ask_next(State, fun(NState) -> NState end);
shutdown_client_next(#state{active=Active, asks=Asks, done=Done,
                            cancels=Cancels, drops=Drops} = State, _,
                     [_, Client]) ->
    case {lists:member(Client, Active), lists:member(Client, Asks)} of
        {true, false} ->
            NState = State#state{active=Active--[Client]},
            dequeue_next(ask_out_next(NState));
        {false, true} ->
            #state{asks=NAsks, drops=NDrops} = NState = ask_aqm_next(State),
            NState2 = NState#state{asks=NAsks--[Client],
                                   drops=NDrops--[Client]},
            dequeue_next(ask_pqm_next(NState2));
        {false, false} ->
            State#state{done=Done--[Client], cancels=Cancels--[Client],
                        drops=Drops--[Client]}
    end.

shutdown_client_post(State, [_, undefined], _) ->
    ask_post(State, fun(NState) -> {true, NState} end);
shutdown_client_post(#state{active=Active, asks=Asks} = State,
                     [_, Client], _) ->
    case {lists:member(Client, Active), lists:member(Client, Asks)} of
        {true, false} ->
            NState = State#state{active=Active--[Client]},
            ask_out_post(NState);
        {false, true} ->
            {Drops, #state{asks=NAsks} = NState} = ask_aqm(State),
            NState2 = NState#state{asks=NAsks--[Client]},
            NState3 = ask_pqm_next(NState2),
            drops_post(Drops -- [Client]) andalso ask_pqm_post(NState2) andalso
            dequeue_post(NState3);
        {false, false} ->
            true
    end.

timeout_args(#state{sregulator=Regulator}) ->
    [Regulator].

timeout_next(State, _, _) ->
    ask_next(dequeue_next(State), fun(NState) -> NState end).

timeout_post(State, _, _) ->
    case dequeue(State) of
        {true, NState} ->
            ask_post(NState, fun(NState2) -> {true, NState2} end);
        false ->
            false
    end.

change_config_args(#state{sregulator=Regulator}) ->
    [Regulator, init()].

change_config(Regulator, Init) ->
    application:set_env(sbroker, ?MODULE, Init),
    sregulator:change_config(Regulator, 100).

change_config_next(State, _, [_, ignore]) ->
    State;
change_config_next(State, _, [_, bad]) ->
    State;
change_config_next(State, _, [_, {ok, {QueueSpec, ValveSpec, _}}]) ->
    NState = ask_change(QueueSpec, State),
    valve_change(ValveSpec, NState).

ask_change({_, AskDrops, AskOut, AskSize, AskDrop},
           #state{ask_drops=AskDrops} = State) ->
    State#state{ask_out=AskOut, ask_size=AskSize, ask_drop=AskDrop};
ask_change({_, AskDrops, AskOut, AskSize, AskDrop}, State) ->
    State#state{ask_out=AskOut, ask_drops=AskDrops, ask_state=AskDrops,
                ask_size=AskSize, ask_drop=AskDrop}.

valve_change({_, ValveStatus, Min, Max},
             #state{valve_status=ValveStatus} = State) ->
    State#state{min=Min, max=Max};
valve_change({_, ValveStatus, Min, Max}, State) ->
    State#state{valve_state=ValveStatus, valve_status=ValveStatus,
                min=Min, max=Max}.

change_config_post(_, [_, ignore], ok) ->
    true;
change_config_post(_, [_, bad],
                   {error, {'EXIT', {bad_return, {?MODULE, init, bad}}}}) ->
    true;
change_config_post(_, [_, {ok, _}], ok) ->
    true;
change_config_post(_, _, _) ->
    false.

%% Helpers

ask_out_next(#state{active=Active} = State) ->
    ask_next(State,
             fun(#state{asks=[]} = NState) ->
                     NState;
                (#state{asks=[Ask | NAsks], ask_out=out} = NState) ->
                     NState#state{asks=NAsks, active=Active++[Ask]};
                (#state{asks=NAsks, ask_out=out_r} = NState) ->
                     Ask = lists:last(NAsks),
                     NState#state{asks=droplast(NAsks), active=Active++[Ask]}
             end).

ask_out_post(#state{active=Active, sregulator=Regulator} = State) ->
    ask_post(State,
             fun(#state{asks=[]} = NState) ->
                     {true, NState};
                (#state{asks=[Ask | NAsks], ask_out=out} = NState) ->
                     NState2 = NState#state{asks=NAsks, active=Active++[Ask]},
                     {go_post(Ask, Regulator), NState2};
                (#state{asks=NAsks, ask_out=out_r} = NState) ->
                     Ask = lists:last(NAsks),
                     NAsks2 = droplast(NAsks),
                     NState2 = NState#state{asks=NAsks2, active=Active++[Ask]},
                     {go_post(Ask, Regulator), NState2}
             end).

ask_next(State, Fun) ->
    NState = ask_aqm_next(State),
    NState2 = Fun(NState),
    dequeue_next(ask_pqm_next(NState2)).

ask_post(State, Fun) ->
    {Drops, NState} = ask_aqm(State),
    {Result, NState2} = Fun(NState),
    Result andalso drops_post(Drops) andalso ask_pqm_post(NState2) andalso
    dequeue_post(ask_pqm_next(NState2)).

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
                drops=Drops} = State) ->
    AskDrop2 = min(length(Asks), AskDrop),
    {Drops2, NAsks} = lists:split(AskDrop2, Asks),
    {Drops2, State#state{asks=NAsks, ask_state=NAskState, drops=Drops++Drops2}}.

ask_pqm_next(#state{asks=Asks, ask_size=AskSize, ask_drop=AskDrop} = State)
  when length(Asks) > AskSize ->
    {Drops, #state{asks=NAsks} = NState} = ask_aqm(State),
    case Drops of
        [] when AskDrop =:= drop ->
            ask_pqm_next(NState#state{asks=dropfirst(NAsks)});
        [] when AskDrop =:= drop_r ->
            ask_pqm_next(NState#state{asks=droplast(NAsks)});
        [_ | _] ->
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
        [_ | _] ->
            ask_pqm_post(NState)
    end;
ask_pqm_post(_) ->
    true.

droplast([]) ->
    [];
droplast(List) ->
    [_ | Rest] = lists:reverse(List),
    lists:reverse(Rest).

dropfirst([]) ->
    [];
dropfirst([_ | Rest]) ->
    Rest.

drops_post([]) ->
    true;
drops_post([Client | Drops]) ->
    case result(Client) of
        {drop, _} ->
            drops_post(Drops);
        Other ->
            ct:pal("~p Drop: ~p", [Client, Other]),
            false
    end.

go_post(Client, Regulator) ->
    case result(Client) of
        {go, Ref, Regulator, SojournTime} when is_reference(Ref) ->
            is_integer(SojournTime) andalso SojournTime >= 0;
        Other ->
            ct:pal("~p Go: ~p", [Client, Other]),
            false
    end.


retry_post(Client) ->
    case result(Client) of
        {retry, SojournTime} ->
            is_integer(SojournTime) andalso SojournTime >= 0;
        Other ->
            ct:pal("~p Retry: ~p", [Client, Other]),
            false
    end.

result(Client) ->
    client_call(Client, result).


dequeue(State) ->
    case dequeue_post(State) of
        true ->
            {true, dequeue_next(State)};
        false ->
            false
    end.

dequeue_next(#state{asks=[_ | _], active=Active, min=Min} = State)
  when length(Active) < Min ->
    case ask_aqm_next(State) of
        #state{asks=[]} = NState ->
            NState;
        #state{asks=NAsks, ask_out=out} = NState ->
            Client = hd(NAsks),
            NState2 = NState#state{asks=dropfirst(NAsks),
                                   active=Active++[Client]},
            dequeue_next(ask_pqm_next(NState2));
        #state{asks=NAsks, ask_out=out_r} = NState ->
            Client = lists:last(NAsks),
            NState2 = NState#state{asks=droplast(NAsks),
                                   active=Active++[Client]},
            dequeue_next(ask_pqm_next(NState2))
    end;
dequeue_next(State) ->
    State.

dequeue_post(#state{asks=[_ | _], active=Active, min=Min,
                    sregulator=Regulator} = State) when length(Active) < Min ->
    case ask_aqm(State) of
        {Drops, #state{asks=[]}} ->
            drops_post(Drops);
        {Drops, #state{asks=NAsks, ask_out=out} = NState} ->
            Client = hd(NAsks),
            NState2 = NState#state{asks=dropfirst(NAsks),
                                   active=Active++[Client]},
            drops_post(Drops) andalso go_post(Client, Regulator) andalso
            ask_pqm_post(NState2) andalso dequeue_post(ask_pqm_next(NState2));
        {Drops, #state{asks=NAsks, ask_out=out_r} = NState} ->
            Client = lists:last(NAsks),
            NState2 = NState#state{asks=droplast(NAsks),
                                   active=Active++[Client]},
            drops_post(Drops) andalso go_post(Client, Regulator) andalso
            ask_pqm_post(NState2) andalso dequeue_post(ask_pqm_next(NState2))
    end;
dequeue_post(_) ->
    true.

valve_status(#state{asks=[], active=Active, min=Min} = State)
  when length(Active) < Min ->
    {closed, State};
valve_status(#state{active=Active, max=Max} = State)
  when length(Active) >= Max ->
    {closed, State};
valve_status(State) ->
    valve_pop(State).

valve_pop(#state{valve_state=[], valve_status=[]} = State) ->
    {closed, State};
valve_pop(#state{valve_state=[], valve_status=ValveStatus} = State) ->
    valve_pop(State#state{valve_state=ValveStatus});
valve_pop(#state{valve_state=[Status | NValveState]} = State) ->
    {Status, State#state{valve_state=NValveState}}.

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

client_init(Regulator, async_ask) ->
    MRef = monitor(process, Regulator),
    Tag = make_ref(),
    {await, Tag, Regulator} = sregulator:async_ask(Regulator, Tag),
    client_init(MRef, Regulator, Tag, make_ref(), queued);
client_init(Regulator, nb_ask) ->
    MRef = monitor(process, Regulator),
    case sregulator:nb_ask(Regulator) of
        {go, Ref, _, _} = State ->
            client_init(MRef, Regulator, make_ref(), Ref, State);
        State ->
            client_init(MRef, Regulator, make_ref(), make_ref(), State)
    end.

client_init(MRef, Regulator, Tag, Ref, State) ->
    proc_lib:init_ack({ok, self(), MRef}),
    client_loop(MRef, Regulator, Tag, Ref, State, []).

client_loop(MRef, Regulator, Tag, Ref, State, Froms) ->
    receive
        {'DOWN', MRef, _, _, _} ->
            exit(normal);
        {Tag, {go, NRef, _, _} = Result} when State =:= queued ->
            _ = [gen:reply(From, Result) || From <- Froms],
            client_loop(MRef, Regulator, Tag, NRef, Result, []);
        {Tag, Result} when State =:= queued ->
            _ = [gen:reply(From, Result) || From <- Froms],
            client_loop(MRef, Regulator, Tag, Ref, Result, []);
        {Tag, Result} when State =/= queued ->
            exit(Regulator, {double_result, {self(), MRef}, State, Result}),
            exit(normal);
        {MRef, From, done} ->
            case sregulator:done(Regulator, Ref, ?TIMEOUT) of
                ok ->
                    gen:reply(From, ok),
                    client_loop(MRef, Regulator, Tag, Ref, done, Froms);
                Error ->
                    gen:reply(From, Error),
                    client_loop(MRef, Regulator, Tag, Ref, State, Froms)
            end;
        {MRef, From, ensure_dropped} ->
            case sregulator:ensure_dropped(Regulator, Ref, ?TIMEOUT) of
                ok ->
                    gen:reply(From, ok),
                    client_loop(MRef, Regulator, Tag, Ref, dropped, Froms);
                Error ->
                    gen:reply(From, Error),
                    client_loop(MRef, Regulator, Tag, Ref, State, Froms)
            end;
        {MRef, From, cancel} ->
            case sregulator:cancel(Regulator, Tag, ?TIMEOUT) of
                ok ->
                    gen:reply(From, ok),
                    client_loop(MRef, Regulator, Tag, Ref, cancelled, Froms);
                Error ->
                    gen:reply(From, Error),
                    client_loop(MRef, Regulator, Tag, Ref, State, Froms)
            end;
        {MRef, From, {update, Response}} ->
            case sregulator:update(Regulator, Ref, Response) of
                {dropped, _} = Dropped ->
                    gen:reply(From, Dropped),
                    client_loop(MRef, Regulator, Tag, Ref, dropped, Froms);
                Other ->
                    gen:reply(From, Other),
                    client_loop(MRef, Regulator, Tag, Ref, State, Froms)
            end;
        {MRef, From, result} when State =:= queued ->
            client_loop(MRef, Regulator, Tag, Ref, State, [From | Froms]);
        {MRef, From, result} ->
            gen:reply(From, State),
            client_loop(MRef, Regulator, Tag, Ref, State, Froms)
    end.
