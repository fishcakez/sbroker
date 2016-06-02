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
-module(sbroker_fq_statem).

-include_lib("proper/include/proper.hrl").

-compile({no_auto_import, [time/0]}).

-export([quickcheck/0]).
-export([quickcheck/1]).
-export([check/1]).
-export([check/2]).

-export([initial_state/0]).
-export([command/1]).
-export([precondition/2]).
-export([next_state/3]).
-export([postcondition/3]).

-export([in/5]).
-export([handle_out_queue/1]).

-record(state, {mod, queue, internal=queue:new(), dict=orddict:new(),
                empty=orddict:new(), index, robin=[], outs=[], cancels=[],
                drops=[], config, time, send_time, timeout_time=infinity}).

-record(list, {reqs, config, actions=[]}).

quickcheck() ->
    quickcheck([]).

quickcheck(Opts) ->
    proper:quickcheck(prop_sbroker_fq(), Opts).

check(CounterExample) ->
    check(CounterExample, []).

check(CounterExample, Opts) ->
    proper:check(prop_sbroker_fq(), CounterExample, Opts).

prop_sbroker_fq() ->
    ?FORALL(Cmds, commands(?MODULE),
            ?TRAPEXIT(begin
                          {History, State, Result} = run_commands(?MODULE, Cmds),
                          ok,
                          ?WHENFAIL(begin
                                        io:format("History~n~p", [History]),
                                        io:format("State~n~p", [State]),
                                        io:format("Result~n~p", [Result])
                                    end,
                                    aggregate(command_names(Cmds), Result =:= ok))
                      end)).

initial_state() ->
    #state{}.

command(#state{queue=undefined} = State) ->
    {call, sbroker_fair_queue, init, init_args(State)};
command(State) ->
    frequency([{20, {call, ?MODULE, in, in_args(State)}},
               {10, {call, sbroker_fair_queue, handle_out,
                     handle_out_args(State)}},
               {4, {call, sbroker_fair_queue, config_change,
                    change_args(State)}},
               {4, {call, sbroker_fair_queue, handle_timeout,
                    timeout_args(State)}},
               {4, {call, sbroker_fair_queue, handle_info, info_args(State)}},
               {4, {call, sbroker_fair_queue, handle_cancel,
                    cancel_args(State)}},
               {4, {call, sbroker_fair_queue, len, len_args(State)}},
               {1, {call, sbroker_fair_queue, terminate,
                    terminate_args(State)}}]).

precondition(State, {call, _, init, Args}) ->
    init_pre(State, Args);
precondition(#state{queue=undefined}, _) ->
    false;
precondition(State, {call, _, in, Args}) ->
    in_pre(State, Args);
precondition(State, {call, _, handle_out, Args}) ->
    handle_out_pre(State, Args);
precondition(State, {call, _, config_change, Args}) ->
    config_change_pre(State, Args);
precondition(State, {call, _, handle_timeout, Args}) ->
    handle_timeout_pre(State, Args);
precondition(State, {call, _, handle_info, Args}) ->
    handle_info_pre(State, Args);
precondition(State, {call, _, handle_cancel, Args}) ->
    handle_cancel_pre(State, Args);
precondition(State, {call, _, len, Args}) ->
    len_pre(State, Args);
precondition(State, {call, _, terminate, Args}) ->
    terminate_pre(State, Args).

next_state(State, Value, {call, _, init, Args}) ->
    init_next(State, Value, Args);
next_state(State, Value, {call, _, in, Args}) ->
    in_next(State, Value, Args);
next_state(State, Value, {call, _, handle_out, Args}) ->
    handle_out_next(State, Value, Args);
next_state(State, Value, {call, _, config_change, Args}) ->
    config_change_next(State, Value, Args);
next_state(State, Value, {call, _, handle_timeout, Args}) ->
    handle_timeout_next(State, Value, Args);
next_state(State, Value, {call, _, handle_info, Args}) ->
    handle_info_next(State, Value, Args);
next_state(State, Value, {call, _, handle_cancel, Args}) ->
    handle_cancel_next(State, Value, Args);
next_state(State, Value, {call, _, len, Args}) ->
    len_next(State, Value, Args);
next_state(State, Value, {call, _, terminate, Args}) ->
    terminate_next(State, Value, Args).

postcondition(State, {call, _, init, Args}, Result) ->
    init_post(State, Args, Result);
postcondition(State, {call, _, in, Args}, Result) ->
    in_post(State, Args, Result);
postcondition(State, {call, _, handle_out, Args}, Result) ->
    handle_out_post(State, Args, Result);
postcondition(State, {call, _, config_change, Args}, Result) ->
    config_change_post(State, Args, Result);
postcondition(State, {call, _, handle_timeout, Args}, Result) ->
    handle_timeout_post(State, Args, Result);
postcondition(State, {call, _, handle_info, Args}, Result) ->
    handle_info_post(State, Args, Result);
postcondition(State, {call, _, handle_cancel, Args}, Result) ->
    handle_cancel_post(State, Args, Result);
postcondition(State, {call, _, len, Args}, Result) ->
    len_post(State, Args, Result);
postcondition(State, {call, _, terminate, Args}, Result) ->
    terminate_post(State, Args, Result).

time() ->
    ?LET(Time, choose(-10, 10),
         erlang:convert_time_unit(Time, milli_seconds, native)).

time(undefined) ->
    time();
time(Time) ->
    oneof([Time,
           ?LET(Incr, choose(5, 5),
                Time + erlang:convert_time_unit(Incr, milli_seconds, native))]).
mod() ->
    oneof([sbroker_fq_queue, sbroker_fq2_queue]).

actions() ->
    resize(4, [{drop(), timeout_incr()}]).

drop() ->
    frequency([{1, choose(1, 2)},
               {2, 0}]).

timeout_incr() ->
    frequency([{1, infinity},
               {2, ?LET(Incr, choose(0, 5),
                        erlang:convert_time_unit(Incr, milli_seconds,
                                                 native))}]).
index() ->
    oneof([node, value, {element, 1}]).

tag() ->
    oneof([a, b, c]).

info() ->
    oneof([{x}, {y}, {z}]).

init_args(#state{time=undefined, queue=undefined, internal=Internal}) ->
    [Internal, time(), {mod(), actions(), index()}];
init_args(#state{time=Time, queue=undefined, internal=Internal}) ->
    [Internal, time(Time), {mod(), actions(), index()}].

init_pre(#state{mod=Mod}, _) when Mod =/= undefined ->
    false;
init_pre(#state{internal=undefined}, _) ->
    false;
init_pre(#state{time=undefined}, _) ->
    true;
init_pre(#state{time=PrevTime}, [_, Time, _]) ->
    PrevTime =< Time.

init_next(State, Value, [_, Time, {Mod, Config, Index}]) ->
    NState = init(Mod, Config, Index, Time, State),
    handle_info_next(NState, Value, [init, Time, init]).

init_post(State, [_, Time, {Mod, Config, Index}], Result) ->
    NState = init(Mod, Config, Index, Time, State),
    handle_info_post(NState, [init, Time, init], Result).

in(SendTime, Tag, Info, Time, Q) ->
    Pid = spawn_client(),
    From = {Pid, Tag},
    {NQ, Timeout} = sbroker_fair_queue:handle_in(SendTime, From, Info, Time, Q),
    {Pid, NQ, Timeout}.

in_args(#state{send_time=SendTime, time=Time, queue=Q}) ->
    [oneof([SendTime, choose(SendTime, Time)]), tag(), info(), time(Time), Q].

in_pre(#state{send_time=PrevSendTime, time=PrevTime},
       [SendTime, _, _, Time, _]) ->
    SendTime =< Time andalso SendTime >= PrevSendTime andalso Time >= PrevTime.

in_next(#state{index=Index} = State, Value, [SendTime, Tag, Info, Time, _]) ->
    Pid = {call, erlang, element, [1, Value]},
    Elem = {SendTime, {Pid, Tag}, Info},
    Q = {call, erlang, element, [2, Value]},
    Key = index(Elem, Index),
    in(Key, Elem, State#state{time=Time, send_time=SendTime, queue=Q}).

in_post(#state{index=Index} = State, [SendTime, Tag, Info, Time, _],
        {Pid, Q, _}) ->
    Elem = {SendTime, {Pid, Tag}, Info},
    Key = index(Elem, Index),
    post(in(Key, Elem, State#state{time=Time, send_time=SendTime, queue=Q})).

handle_out_args(#state{time=Time, queue=Q}) ->
    [time(Time), Q].

handle_out_pre(#state{time=PrevTime}, [Time, _]) ->
    Time >= PrevTime.

handle_out_next(State, Value, [Time, _]) ->
     Q = {call, ?MODULE, handle_out_queue, [Value]},
     case out(State#state{queue=Q, time=Time}) of
         {empty, NState} ->
             NState;
         {Elem, #state{outs=Outs} = NState} ->
             NState#state{outs=Outs++[Elem]}
     end.

handle_out_queue({_, _, _, _, Q, _}) ->
    Q;
handle_out_queue({empty, Q}) ->
    Q.

handle_out_post(State, [Time, _], Result) ->
    {Elem, NState} = out(State#state{time=Time}),
    case Result of
        {empty, Q} when Elem == empty ->
            post(NState#state{queue=Q});
        _ when Elem == empty ->
            ct:pal("Not empty: ~p", [Result]),
            false;
        {SendTime, From, Value, Ref, Q, _} when
              element(1, Elem) == SendTime, element(2, Elem) == From,
              element(3, Elem) == Value, is_reference(Ref) ->
            post(NState#state{queue=Q});
        _ ->
            ct:pal("Out~nExpected: ~p~nObserved: ~p", [Elem, Result]),
            false
    end.

change_args(#state{time=Time, queue=Q}) ->
    [{mod(), actions(), index()}, time(Time), Q].

config_change_pre(#state{time=PrevTime}, [_, Time, _]) ->
    Time >= PrevTime.

config_change_next(#state{mod=Mod, index=Index, config=Config, dict=D,
                          empty=Empty, robin=Robin} = State, Value,
                   [{Mod, Config, Index}, Time, Q]) ->
    NRobin = Robin ++ orddict:fetch_keys(Empty),
    ND = orddict:merge(fun(_, _, _) -> error(conflict) end, D, Empty),
    NState = State#state{dict=ND, empty=orddict:new(), robin=NRobin},
    handle_info_next(NState, Value, [change, Time, Q]);
config_change_next(#state{index=Index, robin=Robin, dict=D,
                          empty=Empty, drops=Drops} = State,
                   Value, [{Mod, Config, Index}, Time, _]) ->
    Q = {call, erlang, element, [1, Value]},
    NRobin = Robin ++ orddict:fetch_keys(Empty),
    ND = orddict:merge(fun(_, _, _) -> error(conflict) end, D, Empty),
    {[], ND2, _} = change([], Time, ND),
    {Drops2, ND3, NTimeout} = change(Config, Time, ND2),
    State#state{mod=Mod, config=Config, time=Time, queue=Q, dict=ND3,
                robin=NRobin, timeout_time=NTimeout, empty=orddict:new(),
                drops=Drops++Drops2};
config_change_next(State, Value, [Args, Time, _]) ->
    NState = State#state{queue=undefined, mod=undefined},
    init_next(NState, Value, [change, Time, Args]).

config_change_post(#state{mod=Mod, index=Index, config=Config, dict=D,
                          empty=Empty, robin=Robin} = State,
                   [{Mod, Config, Index}, Time, Q], Result) ->
    NRobin = Robin ++ orddict:fetch_keys(Empty),
    ND = orddict:merge(fun(_, _, _) -> error(conflict) end, D, Empty),
    NState = State#state{dict=ND, empty=orddict:new(), robin=NRobin},
    handle_info_post(NState, [change, Time, Q], Result);
config_change_post(#state{index=Index, dict=D, empty=Empty,
                          drops=Drops} = State,
                   [{Mod, Config, Index}, Time, _], {Q, NTimeout}) ->
    ND = orddict:merge(fun(_, _, _) -> error(conflict) end, D, Empty),
    {[], ND2, _} = change([], Time, ND),
    case change(Config, Time, ND2) of
        {Drops2, ND3, NTimeout} ->
            post(State#state{mod=Mod, config=Config, time=Time, queue=Q,
                             dict=ND3, timeout_time=NTimeout,
                             empty=orddict:new(), drops=Drops++Drops2});
        {_, _, ExpTimeout} ->
            ct:pal("Timeout~nExpected: ~p~nObserved: ~p~n",
                   [ExpTimeout, NTimeout]),
            false
    end;
config_change_post(State, [Args, Time, _], Result) ->
    NState = State#state{queue=undefined, mod=undefined},
    init_post(NState, [change, Time, Args], Result).

timeout_args(#state{time=Time, queue=Q}) ->
    [time(Time), Q].

handle_timeout_pre(#state{time=PrevTime}, [Time, _]) ->
    Time >= PrevTime.

handle_timeout_next(#state{dict=D, drops=Drops, timeout_time=Timeout} = State,
                    Value, [Time, _]) ->
    {Drops2, ND, NTimeout} = timeout(Timeout, Time, D),
    Q = {call, erlang, element, [1, Value]},
    State#state{queue=Q, time=Time, drops=Drops++Drops2, dict=ND,
                timeout_time=NTimeout}.

handle_timeout_post(#state{dict=D, drops=Drops, timeout_time=Timeout} = State,
                    [Time, _], {Q, NTimeout}) ->
    case timeout(Timeout, Time, D) of
        {Drops2, ND, NTimeout} ->
            post(State#state{queue=Q, time=Time, drops=Drops++Drops2, dict=ND,
                             timeout_time=NTimeout});
        {_, _, ExpTimeout} ->
            ct:pal("Timeout~nExpected: ~p~nObserved: ~p",
                   [ExpTimeout, NTimeout]),
            false
    end.

info_args(#state{time=Time, queue=Q}) ->
    [tag(), time(Time), Q].

handle_info_pre(#state{time=PrevTime}, [_, Time, _]) ->
    Time >= PrevTime.

handle_info_next(State, Value, [_, Time, _]) ->
    Q = {call, erlang, element, [1, Value]},
    info(State#state{time=Time, queue=Q}).

handle_info_post(State, [_, Time, _], {Q, Timeout}) ->
    case info(State#state{time=Time, queue=Q}) of
        #state{timeout_time=Timeout} = NState ->
            post(NState);
        #state{timeout_time=ExpTimeout} ->
            ct:pal("Timeout~nExpected: ~p~nObserved: ~p",
                   [ExpTimeout, Timeout]),
            false
    end.

cancel_args(#state{time=Time, queue=Q}) ->
    [tag(), time(Time), Q].

handle_cancel_pre(#state{time=PrevTime}, [_, Time, _]) ->
    Time >= PrevTime.

handle_cancel_next(State, Value, [Tag, Time, _]) ->
    Q = {call, erlang, element, [2, Value]},
    {_, NState} = cancel(Tag, State#state{queue=Q, time=Time}),
    NState.

handle_cancel_post(State, [Tag, Time, _], {Reply, Q, Timeout}) ->
    case cancel(Tag, State#state{queue=Q, time=Time}) of
        {Reply, #state{timeout_time=Timeout} = NState} ->
            post(NState);
        {Reply, #state{timeout_time=ExpTimeout}} ->
            ct:pal("Timeout~nExpected: ~p~nObserved: ~p",
                   [ExpTimeout, Timeout]),
            false;
        {ExpReply, _} ->
            ct:pal("Reply~nExpected: ~p~nObserved: ~p", [ExpReply, Reply]),
            false
    end.

len_args(#state{queue=Q}) ->
    [Q].

len_pre(_, _) ->
    true.

len_next(State, _, _) ->
    State.

len_post(#state{dict=D}, _, Len) ->
    Length = fun(_, #list{reqs=Reqs}, Size) -> length(Reqs) + Size end,
    case orddict:fold(Length, 0 , D) of
        Len ->
            true;
        ExpLen ->
            ct:pal("Len~nExpected: ~p~nObserved: ~p~n", [ExpLen, Len]),
            false
    end.

terminate_args(#state{queue=Q}) ->
    [oneof([change, stop]), Q].

terminate_pre(_, _) ->
    true.

terminate_next(State, Value, _) ->
    State#state{queue=undefined, mod=undefined, internal=Value}.

terminate_post(#state{dict=D} = State, _, Result) ->
    L = to_list(D),
    ResultL = [{SendTime, From, Info} ||
               {SendTime, From, Info, _} <- queue:to_list(Result)],
    case ResultL of
        L ->
            post(State);
        _ ->
            ct:pal("Terminate queue~nExpected: ~p~nObserved: ~p", [L, ResultL]),
            false
    end.

%% Helpers

index(_, node) ->
    node();
index({_, _, {Key}}, Index) when Index == value; Index == {element, 1} ->
    Key.

to_list(D) ->
    UnsortedList = [Elem || {_, #list{reqs=Reqs}} <- orddict:to_list(D),
                            Elem <- Reqs],
    lists:sort(UnsortedList).

index(Index, Config, List) ->
    In = fun(Elem, D) ->
                 Key = index(Elem, Index),
                 case orddict:find(Key, D) of
                     {ok,  #list{reqs=Reqs} = L} ->
                         NL = L#list{reqs=Reqs++[Elem]},
                         orddict:store(Key, NL, D);
                     error ->
                         L = #list{reqs=[Elem], config=Config},
                         orddict:store(Key, L, D)
                 end
         end,
    lists:foldl(In, orddict:new(), List).

init(Mod, Config, Index, Time, #state{dict=D} = State) ->
    ND = index(Index, Config, to_list(D)),
    State#state{mod=Mod, internal=undefined, config=Config, index=Index,
                time=Time, dict=ND, send_time=Time, empty=orddict:new(),
                robin=lists:reverse(orddict:fetch_keys(ND))}.

in(Key, Elem,
   #state{robin=Robin, timeout_time=Timeout, time=Time, empty=Empty,
          dict=D, config=Config} = State) ->
    case {orddict:find(Key, D), orddict:find(Key, Empty)} of
        {{ok, #list{reqs=Reqs} = L}, error} ->
            {Drops, NL, LTimeout} = timeout(Time, L#list{reqs=Reqs++[Elem]}),
            RestD = orddict:erase(Key, D),
            {Drops2, RestD2, NTimeout} = timeout(Timeout, Time, RestD),
            ND = orddict:store(Key, NL, RestD2),
            State#state{drops=Drops++Drops2, dict=ND,
                        timeout_time=min(LTimeout, NTimeout)};
        {error, {ok, L}} ->
            {Drops, NL, LTimeout} = timeout(Time, L#list{reqs=[Elem]}),
            NEmpty = orddict:erase(Key, Empty),
            {Drops2, ND, NTimeout} = timeout(Timeout, Time, D),
            ND2 = orddict:store(Key, NL, ND),
            State#state{robin=Robin++[Key], drops=Drops++Drops2, dict=ND2,
                        empty=NEmpty, timeout_time=min(LTimeout, NTimeout)};
        {error, error} ->
            L = #list{config=Config, reqs=[Elem]},
            {Drops, NL, LTimeout} = timeout(Time, L),
            {Drops2, ND, NTimeout} = timeout(Timeout, Time, D),
            ND2 = orddict:store(Key, NL, ND),
            State#state{robin=Robin++[Key], drops=Drops++Drops2, dict=ND2,
                        timeout_time=min(LTimeout, NTimeout)}
    end.

out(#state{timeout_time=Timeout, time=Time, robin=Robin}  = State) ->
    {Elem, Rest, NState} = out_loop(Robin, [],
                                    State#state{timeout_time=infinity}),
    #state{drops=NDrops, dict=ND, timeout_time=NTimeout} = NState,
    RestD = orddict:filter(fun(Key, _) -> lists:member(Key, Rest) end, ND),
    {Drops2, RestD2, RestTimeout} = timeout(Timeout, Time, RestD),
    ND2 = orddict:merge(fun(_, _, L) -> L end, ND, RestD2),
    NTimeout2 = min(NTimeout, RestTimeout),
    NState2 = NState#state{drops=NDrops++Drops2, dict=ND2,
                           timeout_time=NTimeout2},
    {Elem, NState2}.

out_loop([], [], #state{timeout_time=infinity} = State) ->
    {empty, [], State#state{robin=[]}};
out_loop([], Acc, #state{timeout_time=infinity} = State) ->
    [FirstEmpty | Robin] = lists:reverse(Acc),
    {empty, [], State#state{robin=Robin++[FirstEmpty]}};
out_loop([Key | Rest], Acc,
         #state{time=Time, drops=Drops, empty=Empty, dict=D,
                timeout_time=Timeout} = State) ->
    L = orddict:fetch(Key, D),
    {LDrops, NL, LTimeout} = timeout(Time, L),
    NDrops = Drops ++ LDrops,
    case NL of
        #list{reqs=[Elem | Reqs]} ->
            NRobin = Rest ++ lists:reverse(Acc, [Key]),
            ND = orddict:store(Key, NL#list{reqs=Reqs}, D),
            NTimeout = min(LTimeout, Timeout),
            NState = State#state{drops=NDrops, robin=NRobin,
                                 dict=ND, timeout_time=NTimeout},
            {Elem, Rest, NState};
        #list{reqs=[]} ->
            ND = orddict:erase(Key, D),
            NEmpty = orddict:store(Key, NL, Empty),
            NState = State#state{drops=NDrops, dict=ND, empty=NEmpty},
            out_loop(Rest, Acc, NState)
    end.

change(Config, Time, D) ->
    Change = fun({Key, L}, {Drops1, Acc, Timeout}) ->
                     NL = L#list{config=Config, actions=Config},
                     {Drops2, NL2, LTimeout} = timeout(Time, NL),
                     NDrops = Drops1 ++ Drops2,
                     NTimeout = min(Timeout, LTimeout),
                     {NDrops, orddict:store(Key, NL2, Acc), NTimeout}
             end,
    lists:foldl(Change, {[], orddict:new(), infinity}, orddict:to_list(D)).

info(#state{dict=D, empty=Empty, time=Time, drops=Drops} = State) ->
    {Drops2, ND, NTimeout} = do_timeout(Time, D),
    {[], NEmpty, _} = do_timeout(Time, Empty),
    State#state{drops=Drops++Drops2, dict=ND, empty=NEmpty,
                timeout_time=NTimeout}.

cancel(Tag, #state{dict=D, time=Time, drops=Drops} = State) ->
    Cancel = fun({Key, #list{reqs=Reqs} = L}, Count) ->
                     NReqs = [Item || {_, {_, Tag2}, _} = Item <- Reqs,
                                      Tag2 =/= Tag],
                     Diff = length(Reqs) - length(NReqs),
                     {{Key, L#list{reqs=NReqs}}, Count + Diff}
             end,
    {DList, Count} = lists:mapfoldl(Cancel, 0, orddict:to_list(D)),
    {Drops2, ND, NTimeout} = do_timeout(Time, orddict:from_list(DList)),
    NState = State#state{drops=Drops++Drops2, dict=ND, timeout_time=NTimeout},
    case Count of
        0 ->
            {false, NState};
        _ ->
            {Count, NState}
    end.

timeout(_, #list{config=[], actions=[]} = L) ->
    {[], L, infinity};
timeout(Time, #list{config=Config, actions=[]} = L) ->
    timeout(Time, L#list{actions=Config});
timeout(Time,
        #list{actions=[{Drop, TimeoutIncr} | Actions], reqs=Reqs} = L) ->
    DropCount = min(Drop, length(Reqs)),
    {DropReqs, NReqs} = lists:split(DropCount, Reqs),
    DropReqs2 = [{Time-SendTime, From, Value} ||
                 {SendTime, From, Value} <- DropReqs],
    NL = L#list{actions=Actions, reqs=NReqs},
    case TimeoutIncr of
        infinity ->
            {DropReqs2, NL, infinity};
        _ ->
            {DropReqs2, NL, Time+TimeoutIncr}
    end.

timeout(Timeout, Time, D) when Time < Timeout ->
    case orddict:is_empty(D) of
        true ->
            {[], D, infinity};
        false ->
            {[], D, Timeout}
    end;
timeout(_, Time, D) ->
    do_timeout(Time, D).

do_timeout(Time, D) ->
    DoTimeout = fun(Key, L, {Drops, ND, Timeout}) ->
                      {LDrops, NL, LTimeout} = timeout(Time, L),
                      NDrops = Drops ++ LDrops,
                      ND2 = orddict:store(Key, NL, ND),
                      {NDrops, ND2, min(Timeout, LTimeout)}
              end,
    orddict:fold(DoTimeout, {[], orddict:new(), infinity}, D).

post(#state{dict=D, outs=Outs, cancels=Cancels, drops=Drops,
            queue=Q} = State) ->
    L = to_list(D),
    lists:all(fun({_, From, _}) -> no_drop_post(From) end,
              L ++ Outs ++ Cancels) andalso
    lists:all(fun({_, From, _}) -> no_monitor(From) end,
              Cancels ++ Drops) andalso
    lists:all(fun({Sojourn, From, _}) -> drop_post(From, Sojourn) end,
              Drops) andalso
    len_post(State, [Q], sbroker_fair_queue:len(Q)).

no_drop_post({Pid, _}) ->
    client_msgs(Pid, []).

no_monitor({Pid, _} = Client) ->
    {monitored_by, Pids} = erlang:process_info(Pid, monitored_by),
    case lists:member(self(), Pids) of
        false ->
            true;
        true ->
            ct:pal("Client ~p still monitored", [Client]),
            false
    end.

drop_post({Pid, Tag}, Sojourn) ->
    client_msgs(Pid, [{Tag, {drop, Sojourn}}]).

spawn_client() ->
    Parent = self(),
    spawn(fun() -> client_init(Parent) end).

client_msgs(Pid, Expected) ->
    try gen:call(Pid, call, msgs, 100) of
        {ok, Expected} ->
            true;
        {ok, Observed} ->
            ct:pal("Pid: ~p~nExpected: ~p~nObserved: ~p~n",
                   [Pid, Expected, Observed]),
            false
    catch
        exit:Reason ->
            ct:pal("Pid ~p exit: ~p", [Pid, Reason]),
            false
    end.

client_init(Parent) ->
    Ref = monitor(process, Parent),
    client_loop(Ref, []).

client_loop(Ref, Msgs) ->
    receive
        {'DOWN', Ref, _, _, _} ->
            exit(shutdown);
        {call, From, msgs} ->
            gen:reply(From, Msgs),
            client_loop(Ref, Msgs);
        Msg ->
            client_loop(Ref, Msgs ++ [Msg])
    end.
