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
-module(sbroker_SUITE).

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
-export([ask_r/1]).
-export([dirty_cancel/1]).
-export([await_timeout/1]).
-export([await_down/1]).
-export([skip_down_match/1]).
-export([user/1]).
-export([statem/1]).

%% common_test api

all() ->
    [{group, simple},
     {group, property}].

suite() ->
    [{timetrap, {seconds, 120}}].

groups() ->
    [{simple,
      [ask, ask_r, dirty_cancel, await_timeout, await_down, skip_down_match,
       user]},
     {property, [statem]}].

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
    Brokers = application:get_env(sbroker, brokers, []),
    _ = application:stop(sbroker),
    ok = application:set_env(sbroker, brokers, []),
    ok = application:start(sbroker),
    [{brokers, Brokers} | init_per_testcase(all, Config)];
init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(user, Config) ->
    _ = application:stop(sbroker),
    ok = application:set_env(sbroker, brokers, ?config(brokers, Config)),
    ok = application:start(sbroker),
    end_per_testcase(all, Config);
end_per_testcase(_TestCase, _Config) ->
    ok.

%% test cases

ask(_) ->
    {ok, Broker} = sbroker_test:start_link(),
    Ref = make_ref(),
    Self = self(),
    {await, Ref, Broker} = sbroker:async_ask_r(Broker, Self, {Self, Ref}),
    1 = sbroker:len_r(Broker),
    {go, Ref2, Self, AskRel, AskTime} = sbroker:ask(Broker),
    0 = sbroker:len(Broker),
    0 = sbroker:len_r(Broker),
    AskRRel = -AskRel,
    {go, Ref2, Self, AskRRel, AskRTime} = sbroker:await(Ref, ?TIMEOUT),
    if
        AskTime =< AskRTime ->
            ok;
        true ->
            exit({bad_sojourn_times, AskTime, AskRTime})
    end.

ask_r(_) ->
    {ok, Broker} = sbroker_test:start_link(),
    Ref = make_ref(),
    Self = self(),
    {await, Ref, Broker} = sbroker:async_ask(Broker, Self, {Self, Ref}),
    1 = sbroker:len(Broker),
    {go, Ref2, Self, AskRRel, AskRTime} = sbroker:ask_r(Broker),
    0 = sbroker:len(Broker),
    0 = sbroker:len_r(Broker),
    AskRel = -AskRRel,
    {go, Ref2, Self, AskRel, AskTime} = sbroker:await(Ref, ?TIMEOUT),
    if
        AskRTime =< AskTime ->
            ok;
        true ->
            exit({bad_sojourn_times, AskRTime, AskTime})
    end.

dirty_cancel(_) ->
    {ok, Broker} = sbroker_test:start_link(),
    {await, TagA, _} = sbroker:async_ask(Broker),
    ok = sbroker:dirty_cancel(Broker, TagA),
    ok = sbroker:dirty_cancel(Broker, TagA),
    {await, TagB, _} = sbroker:async_ask_r(Broker),
    ok = sbroker:dirty_cancel(Broker, TagB),
    ok = sbroker:dirty_cancel(Broker, TagB),
    0 = sbroker:len(Broker),
    0 = sbroker:len_r(Broker),
    receive {TagA, MsgA} -> exit({no_cancel, MsgA}) after 0 -> ok end,
    receive {TagB, MsgB} -> exit({no_cancel, MsgB}) after 0 -> ok end,
    ok.

await_timeout(_) ->
    {ok, Broker} = sbroker_test:start_link(),
    Ref = make_ref(),
    Self = self(),
    {await, Ref, Broker} = sbroker:async_ask(Broker, Self, {Self, Ref}),
    {'EXIT',
     {timeout, {sbroker, await, [Ref, 0]}}} = (catch sbroker:await(Ref, 0)),
    ok.

await_down(_) ->
    Trap = process_flag(trap_exit, true),
    {ok, Broker} = sbroker_test:start_link(),
    {await, Ref, Broker} = sbroker:async_ask(Broker),
    exit(Broker, shutdown),
    receive {'EXIT', Broker, shutdown} -> ok end,
    _ = process_flag(trap_exit, Trap),
    {'EXIT',
     {shutdown,
      {sbroker, await,
       [Ref, ?TIMEOUT]}}} = (catch sbroker:await(Ref, ?TIMEOUT)),
    ok.

skip_down_match(_) ->
    {ok, Broker} = sbroker_test:start_link(),
    ok = sys:suspend(Broker),
    {_, MRef} = spawn_monitor(fun() ->
                          sbroker:async_ask(Broker),
                          exit(normal)
                  end),
    receive
        {'DOWN', MRef, _, _, normal} ->
            Ref = make_ref(),
            Self = self(),
            sys:resume(Broker),
            {await, Ref, _} = sbroker:async_ask_r(Broker, Self, {Self, Ref}),
            {drop, _} = sbroker:await(Ref, ?TIMEOUT),
            ok
    end.

user(_) ->
    Name = {local, ?MODULE},
    Spec1 = {{sbroker_timeout_queue, #{}}, {sbroker_timeout_queue, #{}}, []},
    ok = application:set_env(sbroker, brokers, [{Name, Spec1}]),
    {ok, Pid1} = sbroker_user:start(Name),
    [{sbroker_timeout_queue, ask, _} | _] = sys:get_state(?MODULE),
    [{Name, Pid1, worker, dynamic}] = sbroker_user:which_brokers(),


    ok = sbroker_user:terminate(Name),
    [{Name, undefined, worker, dynamic}] = sbroker_user:which_brokers(),

    ok = sbroker_user:delete(Name),
    [] = sbroker_user:which_brokers(),
    {error, not_found} = sbroker_user:terminate(Name),

    ok = application:set_env(sbroker, brokers, []),
    {ok, undefined} = sbroker_user:start(Name),
    [{Name, undefined, worker, dynamic}] = sbroker_user:which_brokers(),

    ok = application:set_env(sbroker, brokers, [{Name, Spec1}]),
    {ok, Pid2} = sbroker_user:restart(Name),
    [{Name, Pid2, worker, dynamic}] = sbroker_user:which_brokers(),


    ok = application:set_env(sbroker, brokers, [{Name, bad}]),
    [{Name, {bad_return_value, {ok, bad}}}] = sbroker_user:change_config(),

    Spec2 = {{sbroker_drop_queue, #{}}, {sbroker_timeout_queue, #{}}, []},
    ok = application:set_env(sbroker, brokers, [{Name, Spec2}]),
    [] = sbroker_user:change_config(),
    [{sbroker_drop_queue, ask, _} | _] = sys:get_state(?MODULE),

    ok.

statem(Config) ->
    QcOpts = ?config(quickcheck_options, Config),
    case sbroker_statem:quickcheck(QcOpts) of
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
