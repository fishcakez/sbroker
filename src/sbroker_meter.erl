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
-module(sbroker_meter).

-behaviour(sbroker_handlers).

%% public api

-export([change/6]).

%% types

-callback init(Time :: integer(), Args :: any()) -> State :: any().

-callback handle_update(QueueDelay :: non_neg_integer(),
                        ProcessDelay :: non_neg_integer(), Time :: integer(),
                        State :: any()) ->
    {NState :: any(), UpdateTime :: integer() | infinity}.

-callback handle_info(Msg :: any(), Time :: integer(), State :: any()) ->
    {NState :: any(), UpdateTime :: integer() | infinity}.

-callback config_change(Args :: any(), Time :: integer(), State :: any()) ->
    {NState :: any(), UpdateTime :: integer() | infinity}.

-callback terminate(Reason :: sbroker_handler:reason(), State :: any()) ->
    any().

%% @private
-spec change(Module1, State1, Module2, Args, Time, Name) ->
    {ok, State2, TimeoutTime} | {stop, Reason, Callbacks} when
      Module1 :: module(),
      State1 :: any(),
      Module2 :: module(),
      Args :: any(),
      Time :: integer(),
      Name :: sbroker_handler:name(),
      State2 :: any(),
      TimeoutTime :: integer(),
      Reason :: sbroker_handlers:reason(),
      Callbacks :: [{Module1, Reason, State :: any()}].
change(Mod, State, Mod, Args, Now, _) ->
    try Mod:config_change(Args, Now, State) of
        {NState, Next} ->
            {ok, NState, Next};
        Other ->
            Reason = {bad_return_value, Other},
           {stop, Reason, [{Mod, Reason, State}]}
    catch
        Class:Reason ->
            Reason2 = {Class, Reason, erlang:get_stacktrace()},
            {stop, Reason2, [{Mod, Reason2, State}]}
    end;
change(Mod1, State1, Mod2, Args2, Now, Name) ->
    try Mod1:terminate(change, State1) of
        _ ->
            change_init(Mod2, Args2, Now, Name)
    catch
        Class:Reason ->
            Reason2 = {Class, Reason, erlang:get_stacktrace()},
            sbroker_handler:report(?MODULE, handler_crashed, Mod1, Reason2,
                                   State1, Name),
            {stop, Reason2, []}
    end.

%% Internal

change_init(Mod2, Args2, Now, Name) ->
    try Mod2:init(Now, Args2) of
        State2 ->
            {ok, State2, infinity}
    catch
        Class:Reason ->
            Reason2 = {Class, Reason, erlang:get_stacktrace()},
            sbroker_handler:report(?MODULE, start_error, Name, Mod2, Reason2,
                                   Args2),
            {stop, Reason2, []}
    end.
