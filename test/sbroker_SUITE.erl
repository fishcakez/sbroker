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
-export([await_timeout/1]).
-export([await_down/1]).
-export([statem/1]).

%% common_test api

all() ->
    [{group, simple},
     {group, property}].

suite() ->
    [{timetrap, {seconds, 120}}].

groups() ->
    [{simple, [ask, ask_r, await_timeout, await_down]},
     {property, [statem]}].

init_per_suite(Config) ->
    QcOpts = [{numtests, 300}, long_result, {on_output, fun log/2}],
    [{quickcheck_options, QcOpts} | Config].

end_per_suite(_Config) ->
    ok.

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

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%% test cases

ask(_) ->
    {ok, Broker} = sbroker_test:start_link(),
    Ref = make_ref(),
    Self = self(),
    {await, Ref, Broker} = sbroker:async_ask_r(Broker, Ref),
    1 = sbroker:len_r(Broker, ?TIMEOUT),
    {go, Ref2, Self, AskTime} = sbroker:ask(Broker),
    0 = sbroker:len(Broker, ?TIMEOUT),
    0 = sbroker:len_r(Broker, ?TIMEOUT),
    {go, Ref2, Self, AskRTime} = sbroker:await(Ref, ?TIMEOUT),
    if
        AskTime < AskRTime ->
            ok;
        true ->
            exit({bad_sojourn_times, AskTime, AskRTime})
    end.

ask_r(_) ->
    {ok, Broker} = sbroker_test:start_link(),
    Ref = make_ref(),
    Self = self(),
    {await, Ref, Broker} = sbroker:async_ask(Broker, Ref),
    1 = sbroker:len(Broker, ?TIMEOUT),
    {go, Ref2, Self, AskRTime} = sbroker:ask_r(Broker),
    0 = sbroker:len(Broker, ?TIMEOUT),
    0 = sbroker:len_r(Broker, ?TIMEOUT),
    {go, Ref2, Self, AskTime} = sbroker:await(Ref, ?TIMEOUT),
    if
        AskRTime < AskTime ->
            ok;
        true ->
            exit({bad_sojourn_times, AskRTime, AskTime})
    end.

await_timeout(_) ->
    {ok, Broker} = sbroker_test:start_link(),
    Ref = make_ref(),
    {await, Ref, Broker} = sbroker:async_ask(Broker, Ref),
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

statem(Config) ->
    QcOpts = ?config(quickcheck_options, Config),
    case sbroker_statem:quickcheck(QcOpts) of
        true ->
            ok;
        {error, Reason} ->
            error(Reason);
        CounterExample ->
            ct:log("Counter Example:~n~p", [CounterExample]),
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
