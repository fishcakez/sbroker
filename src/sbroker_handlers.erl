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

-callback init(Mod :: module(), BehaviourState :: any(), Time :: integer(),
               Args :: any()) ->
    {State :: any(), TimeoutNext :: integer() | infinity}.

-callback config_change(Mod :: module(), Args :: any(), Time :: integer(),
                        State :: any()) ->
    {NState :: any(), TimeoutNext :: integer() | infinity}.

-callback terminate(Mod :: module(), Reason :: reason(), State :: any()) ->
    BehaviourState :: any().

-export([init/3]).
-export([change/3]).
-export([terminate/3]).
-export([report/6]).
-export([exit_reason/1]).

-type reason() :: stop | change |
    {exit | throw | error, any(), erlang:raise_stacktrace()} |
    {bad_return_value, any()}.

-type name() ::
    {local, atom()} | {global, any()} | {via, module(), any()} |
    {module(), pid()}.

-spec init(Time, Inits, Name) ->
    {ok, Callbacks} | {stop, ExitReason} when
      Time :: integer(),
      Inits :: [{Behaviour, Mod, Args}, ...],
      Behaviour :: module(),
      Mod :: module(),
      Args :: any(),
      Name :: name(),
      Callbacks :: [{Behaviour, Mod, State}, ...],
      State :: any(),
      ExitReason :: any().
init(Time, Inits, Name) ->
    init(Time, Inits, Name, []).

-spec change(Time, Changes, Name) ->
    {ok, Callbacks} | {stop, ExitReason} when
      Time :: integer(),
      Changes :: [{Behaviour, Mod1, State1, Mod2, Args2}, ...],
      Behaviour :: module(),
      Mod1 :: module(),
      State1 :: any(),
      Mod2 :: module(),
      Args2 :: any(),
      Name :: name(),
      TimeoutTime2 :: integer() | infinity,
      Callbacks :: [{Behaviour, Mod2, State2, TimeoutTime2}, ...],
      State2 :: any(),
      ExitReason :: any().
change(Now, Changes, Name) ->
    change(Now, Changes, Name, []).

-spec terminate(Reason, [{Behaviour, Module, ModReason, State}, ...], Name) ->
    {stop, NewExitReason} when
      Reason :: reason() | {stop, ExitReason :: any()},
      Behaviour :: module(),
      Module :: module(),
      ModReason :: reason(),
      State :: any(),
      Name :: name(),
      NewExitReason :: any().
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
        Class:Reason ->
            Reason2 = {Class, Reason, erlang:get_stacktrace()},
            report(Behaviour, handler_crashed, Mod, Reason2, State, Name),
            terminate(Reason2, Rest, Name)
    end.

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
      Reason :: {exit | throw | error, any(), erlang:raise_stacktrace()} |
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

init(_, [], _, Callbacks) ->
    {ok, lists:reverse(Callbacks)};
init(Time, [{Behaviour, Mod, Args} | Inits], Name, Callbacks) ->
    try Behaviour:init(Mod, Behaviour:initial_state(), Time, Args) of
        {State, Next} ->
            NCallbacks = [{Behaviour, Mod, State, Next} | Callbacks],
            init(Time, Inits, Name, NCallbacks);
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

change(_, [], _, Callbacks) ->
    {ok, lists:reverse(Callbacks)};
change(Time, [{Behaviour, Mod1, State1, Mod2, Args2} | Changes], Name,
       Callbacks) ->
    case change(Behaviour, Mod1, State1, Mod2, Args2, Time, Name) of
        {ok, NState, Next} ->
            NCallbacks = [{Behaviour, Mod2, NState, Next} | Callbacks],
            change(Time, Changes, Name, NCallbacks);
        {stop, Reason, Failures} ->
            NCallbacks = [{Behaviour2, Mod, stop, State} ||
                          {Behaviour2, Mod, State, _} <- Callbacks],
            NChanges = [{Behaviour3, Mod, stop, State} ||
                        {Behaviour3, Mod, State, _, _} <- Changes],
            NCallbacks2 = lists:reverse(NCallbacks, Failures ++ NChanges),
            terminate(Reason, NCallbacks2, Name)
    end.


change(Behaviour, Mod, State, Mod, Args, Now, _) ->
    try Behaviour:config_change(Mod, Args, Now, State) of
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
change(Behaviour, Mod1, State1, Mod2, Args2, Now, Name) ->
    try Behaviour:terminate(Mod1, change, State1) of
        Result ->
            change_init(Behaviour, Result, Mod2, Args2, Now, Name)
    catch
        Class:Reason ->
            Reason2 = {Class, Reason, erlang:get_stacktrace()},
            report(Behaviour, handler_crashed, Mod1, Reason, State1, Name),
            {stop, Reason2, []}
    end.

change_init(Behaviour, Result, Mod, Args, Now, Name) ->
    try Behaviour:init(Mod, Result, Now, Args) of
        {NState, Next} ->
            {ok, NState, Next};
        Other ->
            Reason = {bad_return_value, Other},
            report(?MODULE, start_error, Mod, Reason, Args, Name),
            {stop, Reason, []}
    catch
        Class:Reason ->
            Reason2 = {Class, Reason, erlang:get_stacktrace()},
            report(?MODULE, start_error, Mod, Reason2, Args, Name),
            {stop, Reason2, []}
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
    Args = [Tag, Behaviour, Mod, Name, Args, Reason],
    error_logger:format(Format, Args);
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
