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

-callback change(Module1, State1, Module2, TimeUnit, Args, Time, Name) ->
    {ok, State2, TimeoutTime} |
    {stop, Reason, [{Module, ModReason, State}]} when
      Module1 :: module(),
      State1 :: any(),
      Module2 :: module(),
      TimeUnit :: sbroker_time:unit(),
      Args :: any(),
      Time :: integer(),
      Name :: sbroker_handler:name(),
      State2 :: any(),
      TimeoutTime :: integer(),
      Reason :: sbroker_handler:reason(),
      Module :: module(),
      ModReason :: sbroker_handler:reason(),
      State :: any().

-export([init/4]).
-export([change/4]).
-export([terminate/3]).
-export([report/6]).
-export([exit_reason/1]).

-type reason() :: stop | change |
    {exit | throw | error, any(), erlang:raise_stacktrace()} |
    {bad_return_value, any()}.

-type name() ::
    {local, atom()} | {global, any()} | {via, module(), any()} |
    {module(), pid()}.

-spec init(TimeUnit, Time, Inits, Name) ->
    {ok, Callbacks} | {stop, ExitReason} when
      TimeUnit :: sbroker_time:unit(),
      Time :: integer(),
      Inits :: [{Behaviour, Mod, Args}, ...],
      Behaviour :: module(),
      Mod :: module(),
      Args :: any(),
      Name :: name(),
      Callbacks :: [{Behaviour, Mod, State}, ...],
      State :: any(),
      ExitReason :: any().
init(TimeUnit, Time, Inits, Name) ->
    init(TimeUnit, Time, Inits, Name, []).

-spec change(TimeUnit, Time, Changes, Name) ->
    {ok, Callbacks} | {stop, ExitReason} when
      TimeUnit :: sbroker_time:unit(),
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
change(TimeUnit, Now, Changes, Name) ->
    change(TimeUnit, Now, Changes, Name, []).

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
    try Mod:terminate(ModReason, State) of
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

init(_, _, [], _, Callbacks) ->
    {ok, lists:reverse(Callbacks)};
init(TimeUnit, Time, [{Behaviour, Mod, Args} | Inits], Name, Callbacks) ->
    try Mod:init(TimeUnit, Time, Args) of
        State ->
            NCallbacks = [{Behaviour, Mod, State} | Callbacks],
            init(TimeUnit, Time, Inits, Name, NCallbacks)
    catch
        Class:Reason ->
            Stack = erlang:get_stacktrace(),
            Reason2 = {Class, Reason, Stack},
            NCallbacks = [{Behaviour2, Mod2, stop, State2} ||
                          {Behaviour2, Mod2, State2} <- Callbacks],
            terminate(Reason2, NCallbacks, Name)

    end.

change(_, _, [], _, Callbacks) ->
    {ok, lists:reverse(Callbacks)};
change(TimeUnit, Time, [{Behaviour, Mod1, State1, Mod2, Args2} | Changes], Name,
       Callbacks) ->
    case Behaviour:change(Mod1, State1, Mod2, TimeUnit, Args2, Time, Name) of
        {ok, State2, Next2} ->
            NCallbacks = [{Behaviour, Mod2, State2, Next2} | Callbacks],
            change(TimeUnit, Time, Changes, Name, NCallbacks);
        {stop, Reason, Callbacks2} ->
            NCallbacks = [{Behaviour2, Mod, stop, State} ||
                          {Behaviour2, Mod, State, _} <- Callbacks],
            NCallbacks2 = [{Behaviour, Mod, ModReason, State} ||
                           {Mod, ModReason, State} <- Callbacks2],
            NCallbacks3 = [{Behaviour3, Mod, stop, State} ||
                           {Behaviour3, Mod, State, _, _} <- Changes],
            NCallbacks4 = lists:reverse(NCallbacks, NCallbacks2 ++ NCallbacks3),
            terminate(Reason, NCallbacks4, Name)
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
