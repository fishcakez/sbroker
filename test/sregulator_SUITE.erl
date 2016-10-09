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
-module(sregulator_SUITE).

-include_lib("common_test/include/ct.hrl").
-define(TIMEOUT, 5000).

%% common_test api

-export([all/0]).
-export([suite/0]).
-export([groups/0]).
-export([init_per_suite/1]).
-export([end_per_suite/1]).
-export([group/1]).
-export([init_per_group/2]).
-export([end_per_group/2]).
-export([init_per_testcase/2]).
-export([end_per_testcase/2]).

%% test cases

-export([ask/1]).
-export([done/1]).
-export([dirty_done/1]).
-export([dirty_cancel/1]).
-export([continue/1]).
-export([await_timeout/1]).
-export([await_down/1]).
-export([user/1]).
-export([statem/1]).
-export([rate_statem/1]).

%% common_test api

all() ->
    [{group, simple},
     {group, property}].

suite() ->
    [{timetrap, {seconds, 60}}].

groups() ->
    [{simple,
      [ask, done, dirty_done, dirty_cancel, continue, await_timeout,
       await_down, user]},
     {property, [statem, rate_statem]}].

init_per_suite(Config) ->
    {ok, Started} = application:ensure_all_started(sbroker),
    {alarm_handler, Alarms} = sbroker_test_handler:add_handler(),
    QcOpts = [{numtests, 1000}, long_result, {on_output, fun log/2}],
    [{quickcheck_options, QcOpts}, {started, Started}, {alarms, Alarms} |
     Config].

end_per_suite(Config) ->
    try
        Alarms = ?config(alarms, Config),
        {sbroker_test_handler, _} = sbroker_test_handler:delete_handler(),
        _ = [alarm_handler:set_alarm(Alarm) || Alarm <- Alarms],
        ok
    after
        Started = ?config(started, Config),
        _ = [application:stop(App) || App <- Started]
    end.

group(_Group) ->
    [].

init_per_group(property, Config) ->
    case code:is_loaded(proper) of
        {file, _} ->
            init_per_group(all, Config);
        false ->
            case code:load_file(proper) of
                {module, proper} ->
                    init_per_group(all, Config);
                {error, Reason} ->
                    {skip, "Could not load proper: " ++ atom_to_list(Reason)}
            end
    end;
init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(user, Config) ->
    Regulators = application:get_env(sbroker, regulators, []),
    _ = application:stop(sbroker),
    ok = application:set_env(sbroker, regulators, []),
    ok = application:start(sbroker),
    [{regulators, Regulators} | init_per_testcase(all, Config)];
init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(user, Config) ->
    _ = application:stop(sbroker),
    ok = application:set_env(sbroker, regulators, ?config(regulators, Config)),
    ok = application:start(sbroker),
    end_per_testcase(all, Config);
end_per_testcase(_TestCase, _Config) ->
    ok.

%% test cases

ask(_) ->
    {ok, Regulator} = sregulator_test:start_link(),
    Ref = make_ref(),
    {await, Ref, Regulator} = sregulator:async_ask(Regulator, {self(), Ref}),
    1 = sregulator:len(Regulator, ?TIMEOUT),
    ok = sregulator:update(Regulator, -1, ?TIMEOUT),
    0 = sregulator:len(Regulator, ?TIMEOUT),
    1 = sregulator:size(Regulator, ?TIMEOUT),
    {go, _, Regulator, RelativeTime, SojournTime} =
        sregulator:await(Ref, ?TIMEOUT),
    if
        SojournTime >= RelativeTime, SojournTime >= 0 ->
            ok;
        true ->
            exit({bad_times, RelativeTime, SojournTime})
    end.

done(_) ->
    {ok, Regulator} = sregulator_test:start_link(),
    ok = sregulator:update(Regulator, -1, ?TIMEOUT),
    {go, Ref, Regulator, _, _} = sregulator:ask(Regulator),
    {stop, _} = sregulator:done(Regulator, Ref, ?TIMEOUT),
    0 = sregulator:size(Regulator, ?TIMEOUT),
    {not_found, _} = sregulator:done(Regulator, Ref, ?TIMEOUT),
    ok.

dirty_done(_) ->
    {ok, Regulator} = sregulator_test:start_link(),
    ok = sregulator:update(Regulator, -1, ?TIMEOUT),
    {go, Ref, Regulator, _, _} = sregulator:ask(Regulator),
    ok = sregulator:dirty_done(Regulator, Ref),
    0 = sregulator:size(Regulator, ?TIMEOUT),
    ok = sregulator:dirty_done(Regulator, Ref),
    0 = sregulator:size(Regulator, ?TIMEOUT),
    ok.

dirty_cancel(_) ->
    {ok, Regulator} = sregulator_test:start_link(),
    ok = sregulator:update(Regulator, -1, ?TIMEOUT),
    {await, TagA, _} = sregulator:async_ask(Regulator),
    ok = sregulator:dirty_cancel(Regulator, TagA),
    {await, TagB, _} = sregulator:async_ask(Regulator),
    ok = sregulator:dirty_cancel(Regulator, TagB),
    0 = sregulator:len(Regulator, ?TIMEOUT),
    receive {TagA, {go, _, _, _, _}} -> ok end,
    receive {TagB, Msg} -> exit({no_cancel, Msg}) after 0 -> ok end,
    ok.

continue(_) ->
    {ok, Regulator} = sregulator_test:start_link(),
    ok = sregulator:update(Regulator, -1, ?TIMEOUT),
    {go, Ref, Regulator, _, _} = sregulator:ask(Regulator),
    ok = sregulator:update(Regulator, -1, ?TIMEOUT),
    {go, Ref, Regulator, _, SojournTime} = sregulator:continue(Regulator, Ref),
    if
        SojournTime >= 0 -> ok;
        true ->  exit({bad_sojourn_time, SojournTime})
    end,
    1 = sregulator:size(Regulator, ?TIMEOUT),
    {stop, _} = sregulator:continue(Regulator, Ref),
    0 = sregulator:size(Regulator, ?TIMEOUT),
    {not_found, _} = sregulator:continue(Regulator, Ref),
    0 = sregulator:size(Regulator, ?TIMEOUT),
    ok.

await_timeout(_) ->
    {ok, Regulator} = sregulator_test:start_link(),
    Ref = make_ref(),
    {await, Ref, Regulator} = sregulator:async_ask(Regulator, {self(), Ref}),
    {'EXIT',
     {timeout, {sregulator, await, [Ref, 0]}}} =
    (catch sregulator:await(Ref, 0)),
    ok.

await_down(_) ->
    Trap = process_flag(trap_exit, true),
    {ok, Regulator} = sregulator_test:start_link(),
    {await, Ref, Regulator} = sregulator:async_ask(Regulator),
    exit(Regulator, shutdown),
    receive {'EXIT', Regulator, shutdown} -> ok end,
    _ = process_flag(trap_exit, Trap),
    {'EXIT',
     {shutdown,
      {sregulator, await,
       [Ref, ?TIMEOUT]}}} = (catch sregulator:await(Ref, ?TIMEOUT)),
    ok.

user(_) ->
    Name = {local, ?MODULE},
    Spec1 = {{sbroker_timeout_queue, #{}}, {sregulator_open_valve, #{}}, []},
    ok = application:set_env(sbroker, regulators, [{Name, Spec1}]),
    {ok, Pid1} = sregulator_user:start(Name),
    [{sbroker_timeout_queue, queue, _} | _] = sys:get_state(?MODULE),
    [{Name, Pid1, worker, dynamic}] = sregulator_user:which_regulators(),


    ok = sregulator_user:terminate(Name),
    [{Name, undefined, worker, dynamic}] = sregulator_user:which_regulators(),

    ok = sregulator_user:delete(Name),
    [] = sregulator_user:which_regulators(),
    {error, not_found} = sregulator_user:terminate(Name),

    ok = application:set_env(sbroker, regulators, []),
    {ok, undefined} = sregulator_user:start(Name),
    [{Name, undefined, worker, dynamic}] = sregulator_user:which_regulators(),

    ok = application:set_env(sbroker, regulators, [{Name, Spec1}]),
    {ok, Pid2} = sregulator_user:restart(Name),
    [{Name, Pid2, worker, dynamic}] = sregulator_user:which_regulators(),


    ok = application:set_env(sbroker, regulators, [{Name, bad}]),
    [{Name, {bad_return_value, {ok, bad}}}] = sregulator_user:change_config(),

    Spec2 = {{sbroker_drop_queue, #{}}, {sregulator_open_valve, #{}}, []},
    ok = application:set_env(sbroker, regulators, [{Name, Spec2}]),
    [] = sregulator_user:change_config(),
    [{sbroker_drop_queue, queue, _} | _] = sys:get_state(?MODULE),

    ok.

statem(Config) ->
    QcOpts = ?config(quickcheck_options, Config),
    case sregulator_statem:quickcheck(QcOpts) of
        true ->
            ok;
        {error, Reason} ->
            error(Reason);
        CounterExample ->
            ct:pal("Counter Example:~n~p", [CounterExample]),
            error(counterexample)
    end.

rate_statem(Config) ->
    QcOpts = ?config(quickcheck_options, Config),
    case sregulator_rate_statem:quickcheck([{numtests, 100} | QcOpts]) of
        true ->
            ok;
        {error, Reason} ->
            error(Reason);
        CounterExample ->
            ct:pal("Counter Example:~n~p", [CounterExample]),
            error(counterexample)
    end.

%% Custom log format.
log(".", []) ->
    ok;
log("!", []) ->
    ok;
log("OK: " ++ Comment = Format, Args) ->
    ct:comment(no_trailing_newline(Comment), Args),
    io:format(no_trailing_newline(Format), Args);
log("Failed: " ++ Comment = Format, Args) ->
    ct:comment(no_trailing_newline(Comment), Args),
    io:format(no_trailing_newline(Format), Args);
log(Format, Args) ->
    io:format(no_trailing_newline(Format), Args).

no_trailing_newline(Format) ->
    try lists:split(length(Format) - 2, Format) of
        {Format2, "~n"} ->
            Format2;
        _ ->
            Format
    catch
        error:badarg ->
            Format
    end.
