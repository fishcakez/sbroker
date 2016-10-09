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
%% @private
-module(sbroker_handlers).

-callback initial_state() -> BehaviourState :: any().

-callback init(Mod :: module(), BehaviourState :: any(), Send :: integer(),
               Time :: integer(), Args :: any()) ->
    {State :: any(), TimeoutNext :: integer() | infinity}.

-callback code_change(Mod :: module(), OldVsn ::any(), Send :: integer(),
                      Time :: integer(), State :: any(), Extra :: any()) ->
    {NState :: any(), TimeoutNext :: integer() | infinity}.

-callback config_change(Mod :: module(), Args :: any(), Send :: integer(),
                        Time :: integer(), State :: any()) ->
    {NState :: any(), TimeoutNext :: integer() | infinity}.

-callback terminate(Mod :: module(), Reason :: reason(), State :: any()) ->
    BehaviourState :: any().

-export([init/5]).
-export([meters_info/4]).
-export([meters_update/6]).
-export([code_change/7]).
-export([config_change/6]).
-export([terminate/4]).
-export([report/6]).
-export([exit_reason/1]).

-type reason() :: stop | change |
    {exit | throw | error, any(), stacktrace()} |
    {bad_return_value, any()}.

-export_type([reason/0]).

-type name() ::
    {local, atom()} | {global, any()} | {via, module(), any()} |
    {module(), pid()}.

-type stacktrace() ::
    [{module(), atom(), arity() | [term()]} | {function(), [term()]}] |
    [{module(), atom(), arity() | [term()], [{atom(),term()}]} |
     {function(), [term()], [{atom(),term()}]}].

-spec init(Send, Time, Inits, MeterArgs, Name) ->
    {ok, Callbacks, {Meters, TimeoutTime}} | {stop, ExitReason} when
      Send :: integer(),
      Time :: integer(),
      Inits :: [{Behaviour, Mod, Args}, ...],
      Behaviour :: module(),
      Mod :: module(),
      Args :: any(),
      MeterArgs :: [{MeterMod, MeterArg}],
      MeterMod :: module(),
      MeterArg :: any(),
      Name :: name(),
      Callbacks :: [{Behaviour, Mod, State, TimeoutTime}, ...],
      State :: any(),
      TimeoutTime :: integer() | infinity,
      ExitReason :: any(),
      Meters :: [{MeterMod, MeterState}],
      MeterState :: any().
init(Send, Time, Inits, Meters, Name) ->
    init(Send, Time, Inits, Meters, Name, []).

-spec meters_info(Msg, Time, Meters, Name) ->
    {ok, NMeters, TimeoutTime} | {stop, ExitReason} when
      Msg :: any(),
      Time :: integer(),
      Meters :: [{Mod, State}],
      Mod :: module(),
      State :: any(),
      Name :: name(),
      NMeters :: [{Mod, State}],
      TimeoutTime :: integer() | infinity,
      ExitReason :: any().
meters_info(Msg, Time, Meters, Name) ->
    meters_info(Msg, Time, Meters, Name, [], infinity).

-spec meters_update(QueueDelay, ProcessDelay, RelativeTime, Time, Meters,
                    Name) ->
    {ok, NMeters, TimeoutTime} | {stop, ExitReason} when
      QueueDelay :: non_neg_integer(),
      ProcessDelay :: non_neg_integer(),
      RelativeTime :: integer(),
      Time :: integer(),
      Meters :: [{Mod, State}],
      Mod :: module(),
      State :: any(),
      Name :: name(),
      NMeters :: [{Mod, NState}],
      NState :: any(),
      TimeoutTime :: integer() | infinity,
      ExitReason :: any().
meters_update(QDelay, PDelay, RTime, Time, Meters, Name) ->
    meters_update(QDelay, PDelay, RTime, Time, Meters, Name, [], infinity).

-spec code_change(Send, Time, Callbacks, Meters, ChangeMod, OldVsn, Extra) ->
    {NCallbacks, {NMeters, NTimeoutTime}} when
      Send :: integer(),
      Time :: integer(),
      Callbacks :: [{Behaviour, Mod, State, TimeoutTime}, ...],
      Behaviour :: module(),
      Mod :: module(),
      State :: any(),
      TimeoutTime :: integer() | infinity,
      Meters :: [{MeterMod, MeterState}],
      MeterMod :: module(),
      MeterState :: any(),
      ChangeMod :: module(),
      OldVsn :: any(),
      Extra :: any(),
      NCallbacks :: [{Behaviour, Mod, NState, NTimeoutTime}, ...],
      NState :: any(),
      NTimeoutTime :: integer() | infinity,
      NMeters :: [{MeterMod, NMeterState}],
      NMeterState :: any().
code_change(Send, Now, Callbacks, Meters, ChangeMod, OldVsn, Extra) ->
    NCallbacks = do_code_change(Send, Now, Callbacks, ChangeMod, OldVsn, Extra,
                                []),
    Meters2 = [{sbroker_meter, Mod, State, infinity} || {Mod, State} <- Meters],
    Meters3 = do_code_change(Send, Now, Meters2, ChangeMod, OldVsn, Extra, []),
    Split = fun({sbroker_meter, Mod, State, ModNext}, AccNext) ->
                    {{Mod, State}, min(ModNext, AccNext)}
            end,
    {NCallbacks, lists:mapfoldl(Split, infinity, Meters3)}.

-spec config_change(Send, Time, Changes, Meters, MeterArgs, Name) ->
    {ok, Callbacks, {NMeters, TimeoutTime}} | {stop, ExitReason} when
      Send :: integer(),
      Time :: integer(),
      Changes :: [{Behaviour, Mod1, State1, Mod2, Args2}, ...],
      Behaviour :: module(),
      Mod1 :: module(),
      State1 :: any(),
      Mod2 :: module(),
      Args2 :: any(),
      Meters :: [{MeterMod, MeterState}],
      MeterMod :: module(),
      MeterState :: any(),
      MeterArgs :: [{MeterMod, MeterArg}],
      MeterArg :: any(),
      Name :: name(),
      Callbacks :: [{Behaviour, Mod2, State2, TimeoutTime}, ...],
      State2 :: any(),
      TimeoutTime :: integer() | infinity,
      NMeters :: [{MeterMod, NMeterState}],
      NMeterState :: any(),
      ExitReason :: any().
config_change(Send, Now, Changes, Meters, MeterArgs, Name) ->
    case change(Send, Now, Changes, Name, []) of
        {ok, Callbacks} ->
            meters_change(Now, Meters, MeterArgs, Callbacks, Name);
        {stop, _} = Stop ->
            terminate(Stop, meters_callbacks(Meters), Name)
    end.

-spec terminate(Reason, [{Behaviour, Module, ModReason, State}, ...],
                [{Module, State}], Name) ->
    {stop, NewExitReason} when
      Reason :: reason() | {stop, ExitReason :: any()},
      Behaviour :: module(),
      Module :: module(),
      ModReason :: reason(),
      State :: any(),
      Name :: name(),
      NewExitReason :: any().
terminate(Reason, Callbacks, Meters, Name) ->
    Meters2 = [{sbroker_meter, Mod, stop, State} || {Mod, State} <- Meters],
    terminate(Reason, Callbacks ++ Meters2, Name).

-spec report(Behaviour, Type, Mod, Reason, State, Name) -> ok when
      Behaviour :: module(),
      Type :: start_error | handler_crashed,
      Mod :: module(),
      Reason :: reason(),
      State :: any(),
      Name :: {local, atom()} | {global, any()} | {via, module(), any()} |
              {module(), pid()}.
report(Behaviour, Type, Mod, Reason, State, Name) ->
    NReason = report_reason(Reason),
    NName = report_name(Name),
    send_report(Behaviour, Type, Mod, NReason, State, NName).

-spec exit_reason(Reason) -> ExitReason when
      Reason :: {exit | throw | error, any(), stacktrace()} |
        {bad_return_value, any()},
      ExitReason :: any().
exit_reason({throw, Value, Stack}) ->
    {{nocatch, Value}, Stack};
exit_reason({exit, Reason, _}) ->
    Reason;
exit_reason({error, Reason, Stack}) ->
    {Reason, Stack};
exit_reason({bad_return_value, _} = Bad) ->
    Bad.

%% Internal

init(_, Time, [], Meters, Name, Callbacks) ->
    meters_init(Time, Meters, lists:reverse(Callbacks), Name);
init(Send, Time, [{Behaviour, Mod, Args} | Inits], Meters, Name, Callbacks) ->
    try Behaviour:init(Mod, Behaviour:initial_state(), Send, Time, Args) of
        {State, Next} ->
            NCallbacks = [{Behaviour, Mod, State, Next} | Callbacks],
            init(Send, Time, Inits, Meters, Name, NCallbacks);
        Other ->
            Reason = {bad_return_value, Other},
            NCallbacks = [{Behaviour2, Mod2, stop, State2} ||
                          {Behaviour2, Mod2, State2, _} <- Callbacks],
            terminate(Reason, NCallbacks, Name)
    catch
        Class:Reason ->
            Stack = erlang:get_stacktrace(),
            Reason2 = {Class, Reason, Stack},
            NCallbacks = [{Behaviour2, Mod2, stop, State2} ||
                          {Behaviour2, Mod2, State2, _} <- Callbacks],
            terminate(Reason2, NCallbacks, Name)

    end.

meters_init(Time, MeterArgs, Callbacks, Name) ->
    case meters_init(Time, MeterArgs, Name, [], infinity) of
        {ok, Meters, Next} ->
            {ok, Callbacks, {Meters, Next}};
        {stop, Reason, Callbacks2} ->
            NCallbacks = [{Behaviour, Mod, stop, State} ||
                          {Behaviour, Mod, State, _} <- Callbacks],
            terminate(Reason, Callbacks2 ++ NCallbacks, Name)
    end.

meters_init(Time, [{Mod, Args} | Inits], Name, Meters, Next) ->
    try Mod:init(Time, Args) of
        {State, ModNext} ->
            NMeters = [{Mod, State} | Meters],
            NNext = min(ModNext, Next),
            meters_init(Time, Inits, Name, NMeters, NNext);
        Other ->
            Reason = {bad_return_value, Other},
            report(sbroker_meter, start_error, Mod, Reason, Args, Name),
            {stop, Reason, meters_callbacks(Meters)}
    catch
        Class:Reason ->
            Reason2 = {Class, Reason, erlang:get_stacktrace()},
            report(sbroker_meter, start_error, Mod, Reason2, Args, Name),
            {stop, Reason2, meters_callbacks(Meters)}
    end;
meters_init(_, [], _, Meters, Next) ->
    {ok, Meters, Next}.

meters_info(_, _, [], _, NMeters, Next) ->
    {ok, NMeters, Next};
meters_info(Msg, Time, [{Mod, State} | Meters], Name, NMeters, Next) ->
    try Mod:handle_info(Msg, Time, State) of
        {NState, ModNext} ->
            NMeters2 = [{Mod, NState} | NMeters],
            NNext = min(Next, ModNext),
            meters_info(Msg, Time, Meters, Name, NMeters2, NNext);
        Other ->
            Reason = {bad_return_value, Other},
            meters_terminate(Reason, Mod, State, Meters, NMeters, Name)
    catch
        Class:Reason ->
            Stack = erlang:get_stacktrace(),
            Reason2 = {Class, Reason, Stack},
            meters_terminate(Reason2, Mod, State, Meters, NMeters, Name)
    end.

meters_update(_, _, _, _, [], _, NMeters, Next) ->
    {ok, NMeters, Next};
meters_update(QDelay, PDelay, RTime, Time, [{Mod, State} | Meters], Name,
              NMeters, Next) ->
    try Mod:handle_update(QDelay, PDelay, RTime, Time, State) of
        {NState, ModNext} ->
            NMeters2 = [{Mod, NState} | NMeters],
            NNext = min(Next, ModNext),
            meters_update(QDelay, PDelay, RTime, Time, Meters, Name, NMeters2,
                          NNext);
        Other ->
            Reason = {bad_return_value, Other},
            meters_terminate(Reason, Mod, State, Meters, NMeters, Name)
    catch
        Class:Reason ->
            Stack = erlang:get_stacktrace(),
            Reason2 = {Class, Reason, Stack},
            meters_terminate(Reason2, Mod, State, Meters, NMeters, Name)
    end.

meters_terminate(Reason, Mod, State, Meters, NMeters, Name) ->
    Meters2 = [{sbroker_meter, Mod2, stop, State2} ||
               {Mod2, State2} <- Meters],
    Meters3 = [{sbroker_meter, Mod2, stop, State2} ||
               {Mod2, State2} <- NMeters],
    Meters4 = [{sbroker_meter, Mod, Reason, State} | Meters3 ++ Meters2],
    terminate(Reason, Meters4, Name).

do_code_change(_, _, [], _, _, _, Callbacks) ->
    lists:reverse(Callbacks);
do_code_change(Send, Time, [{Behaviour, ChangeMod, State, _} | Callbacks],
               ChangeMod, OldVsn, Extra, NCallbacks) ->
    try Behaviour:code_change(ChangeMod, OldVsn, Send, Time, State, Extra) of
        {NState, NNext} ->
            NCallbacks2 = [{Behaviour, ChangeMod, NState, NNext} | NCallbacks],
            do_code_change(Send, Time, Callbacks, ChangeMod, OldVsn, Extra,
                           NCallbacks2)
    catch
        throw:Value ->
            erlang:raise(error, {nocatch, Value}, erlang:get_stacktrace())
    end;
do_code_change(Send, Time, [Callback | Callbacks], ChangeMod, OldVsn, Extra,
               NCallbacks) ->
    NCallbacks2 = [Callback | NCallbacks],
    do_code_change(Send, Time, Callbacks, ChangeMod, OldVsn, Extra,
                   NCallbacks2).

change(_, _, [], _, Callbacks) ->
    {ok, lists:reverse(Callbacks)};
change(Send, Time, [{Behaviour, Mod1, State1, Mod2, Args2} | Changes], Name,
       Callbacks) ->
    case change(Behaviour, Mod1, State1, Mod2, Args2, Send, Time, Name) of
        {ok, NState, Next} ->
            NCallbacks = [{Behaviour, Mod2, NState, Next} | Callbacks],
            change(Send, Time, Changes, Name, NCallbacks);
        {stop, Reason, Failures} ->
            NCallbacks = [{Behaviour2, Mod, stop, State} ||
                          {Behaviour2, Mod, State, _} <- Callbacks],
            NChanges = [{Behaviour3, Mod, stop, State} ||
                        {Behaviour3, Mod, State, _, _} <- Changes],
            NCallbacks2 = lists:reverse(NCallbacks, Failures ++ NChanges),
            terminate(Reason, NCallbacks2, Name)
    end.

change(Behaviour, Mod, State, Mod, Args, Send, Now, _) ->
    try Behaviour:config_change(Mod, Args, Send, Now, State) of
        {NState, Next} ->
            {ok, NState, Next};
        Other ->
            Reason = {bad_return_value, Other},
            {stop, Reason, [{Behaviour, Mod, Reason, State}]}
    catch
        Class:Reason ->
            Reason2 = {Class, Reason, erlang:get_stacktrace()},
            {stop, Reason2, [{Behaviour, Mod, Reason2, State}]}
    end;
change(Behaviour, Mod1, State1, Mod2, Args2, Send, Now, Name) ->
    try Behaviour:terminate(Mod1, change, State1) of
        Result ->
            change_init(Behaviour, Result, Mod2, Args2, Send, Now, Name)
    catch
        Class:Reason ->
            Reason2 = {Class, Reason, erlang:get_stacktrace()},
            report(Behaviour, handler_crashed, Mod1, Reason2, State1, Name),
            {stop, Reason2, []}
    end.

change_init(Behaviour, Result, Mod, Args, Send, Now, Name) ->
    try Behaviour:init(Mod, Result, Send, Now, Args) of
        {NState, Next} ->
            {ok, NState, Next};
        Other ->
            Reason = {bad_return_value, Other},
            report(Behaviour, start_error, Mod, Reason, Args, Name),
            {stop, Reason, []}
    catch
        Class:Reason ->
            Reason2 = {Class, Reason, erlang:get_stacktrace()},
            report(Behaviour, start_error, Mod, Reason2, Args, Name),
            {stop, Reason2, []}
    end.

meters_change(Time, Meters, MeterArgs, Callbacks, Name) ->
    case meters_change(Time, Meters, MeterArgs, Name) of
        {ok, NMeters, Next} ->
            {ok, Callbacks, {NMeters, Next}};
        {stop, Reason, Callbacks2} ->
            NCallbacks = [{Behaviour, Mod, stop, State} ||
                          {Behaviour, Mod, State, _} <- Callbacks],
            terminate(Reason, Callbacks2 ++ NCallbacks, Name)
    end.

meters_change(Time, Meters, MeterArgs, Name) ->
    {Inits, Changes, Terminates} = split_meters(Meters, MeterArgs),
    case meters_change_term(Terminates, Name) of
        ok ->
            meters_change_init(Time, Inits, Changes, Name);
        {stop, Reason, Callbacks} ->
            {stop, Reason, Callbacks ++ meters_change_callbacks(Changes)}
    end.

split_meters(Meters, MeterArgs) ->
    split_meters(Meters, MeterArgs, [], []).

split_meters([{Mod, State} | Meters], MeterArgs, Changes, Terminates) ->
    case lists:keytake(Mod, 1, MeterArgs) of
        {value, {_, Args}, NMeterArgs} ->
            NChanges = [{Mod, Args, State} | Changes],
            split_meters(Meters, NMeterArgs, NChanges, Terminates);
        false ->
            NTerminates = [{Mod, State} | Terminates],
            split_meters(Meters, MeterArgs, Changes, NTerminates)
    end;
split_meters([], Inits, Changes, Terminates) ->
    {Inits, Changes, Terminates}.

meters_change_term([{Mod, State} | Meters], Name) ->
    try Mod:terminate(stop, State) of
        _ ->
            meters_change_term(Meters, Name)
    catch
        Class:Reason ->
            Reason2 = {Class, Reason, erlang:get_stacktrace()},
            report(sbroker_meter, handler_crashed, Mod, Reason2, State, Name),
            {stop, Reason2, meters_callbacks(Meters)}
    end;
meters_change_term([], _) ->
    ok.

meters_change_init(Time, Inits, Changes, Name) ->
    case meters_init(Time, Inits, Name, [], infinity) of
        {ok, Meters, Next} ->
            meters_change_config(Time, Changes, Meters, Next);
        {stop, Reason, Callbacks} ->
            NChanges = [{sbroker_meter, Mod2, stop, State2} ||
                        {Mod2, _, State2} <- Changes],
            {stop, Reason, Callbacks ++ NChanges}
    end.

meters_change_config(Time, [{Mod, Args, State} | Changes], Meters, Next) ->
    try Mod:config_change(Args, Time, State) of
        {NState, ModNext} ->
            NMeters = [{Mod, NState} | Meters],
            NNext = min(ModNext, Next),
            meters_change_config(Time, Changes, NMeters, NNext);
        Other ->
            Reason = {bad_return_Value, Other},
            Callbacks = meters_callbacks(Meters),
            NChanges = meters_change_callbacks(Changes),
            NChanges2 = [{sbroker_meter, Mod, Reason, State} | NChanges],
            {stop, Reason, State, NChanges2 ++ Callbacks}
    catch
        Class:Reason ->
            Reason2 = {Class, Reason, erlang:get_stacktrace()},
            Callbacks = meters_callbacks(Meters),
            NChanges = meters_change_callbacks(Changes),
            NChanges2 = [{sbroker_meter, Mod, Reason2, State} | NChanges],
            {stop, Reason2, State, NChanges2 ++ Callbacks}
    end;
meters_change_config(_, [], Meters, Next) ->
    {ok, Meters, Next}.

meters_callbacks(Meters) ->
    [{sbroker_meter, Mod, stop, State} || {Mod, State} <- Meters].

meters_change_callbacks(Changes) ->
    [{sbroker_meter, Mod, stop, State} || {Mod, _, State} <- Changes].

terminate({stop, _} = Stop, [], _) ->
    Stop;
terminate(Reason, [], _) ->
    {stop, exit_reason(Reason)};
terminate(Reason, [{Behaviour, Mod, ModReason, State} | Rest], Name) ->
    try Behaviour:terminate(Mod, ModReason, State) of
        _ ->
            maybe_report(Behaviour, Mod, ModReason, State, Name),
            terminate(Reason, Rest, Name)
    catch
        Class:NReason ->
            NReason2 = {Class, NReason, erlang:get_stacktrace()},
            report(Behaviour, handler_crashed, Mod, NReason2, State, Name),
            terminate(NReason2, Rest, Name)
    end.

report_name({local, Name}) when is_atom(Name) ->
    Name;
report_name({global, Name}) ->
    Name;
report_name({via, _, Name}) ->
    Name;
report_name({_, Pid} = Name) when is_pid(Pid) ->
    Name.

report_reason({exit, Reason, Stack}) ->
    {Reason, Stack};
report_reason(Reason) ->
    exit_reason(Reason).

maybe_report(_, _, stop, _, _) ->
    ok;
maybe_report(_, _, change, _, _) ->
    ok;
maybe_report(Behaviour, Mod, Reason, State, Name) ->
    case exit_reason(Reason) of
        normal ->
            ok;
        shutdown ->
            ok;
        {shutdown, _} ->
            ok;
        _ ->
            report(Behaviour, handler_crashed, Mod, Reason, State, Name)
    end.

send_report(Behaviour, start_error, Mod, Reason, Args, Name) ->
    Tag = {Behaviour, start_error},
    Format = "~i** ~p ~p failed to install.~n"
             "** Was installing in ~p~n"
             "** When arguments == ~p~n"
             "** Reason == ~p~n",
    FormatArgs = [Tag, Behaviour, Mod, Name, Args, Reason],
    error_logger:format(Format, FormatArgs);
send_report(Behaviour, handler_crashed, Mod, Reason, State, Name) ->
    Tag = {Behaviour, handler_crashed},
    Format = "~i** ~p ~p crashed.~n"
             "** Was installed in ~p~n"
             "** When state == ~p~n"
             "** Reason == ~p~n",
    NState = format_state(Mod, State),
    Args = [Tag, Behaviour, Mod, Name, NState, Reason],
    error_logger:format(Format, Args).

format_state(Mod, State) ->
    case erlang:function_exported(Mod, format_status, 2) of
        true ->
            try Mod:format_status(terminate, [get(), State]) of
                Status ->
                    Status
            catch
                _:_ ->
                    State
            end;
        false ->
            State
    end.
