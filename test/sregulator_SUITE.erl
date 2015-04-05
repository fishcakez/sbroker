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
-export([len/1]).
-export([size/1]).
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
    [{simple, [ask, len, size, await_timeout, await_down]},
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
    {ok, Regulator} = sregulator_test:start_link(),
    {go, Ref, Regulator, 0} = sregulator:ask(Regulator),
    ok = sregulator:done(Regulator, Ref),
    ok.

len(_) ->
    {ok, Regulator} = sregulator_test:start_link(),
    {go, _, Regulator, 0} = sregulator:ask(Regulator),
    0 = sregulator:len(Regulator, ?TIMEOUT),
    Ref = make_ref(),
    {await, Ref, Regulator} = sregulator:async_ask(Regulator, Ref),
    1 = sregulator:len(Regulator, ?TIMEOUT),
    {await, Ref, Regulator} = sregulator:async_ask(Regulator, Ref),
    2 = sregulator:len(Regulator, ?TIMEOUT),
    ok.

size(_) ->
    {ok, Regulator} = sregulator_test:start_link(),
    0 = sregulator:size(Regulator, ?TIMEOUT),
    {go, _, Regulator, 0} = sregulator:ask(Regulator),
    1 = sregulator:size(Regulator, ?TIMEOUT),
    Ref = make_ref(),
    {await, Ref, Regulator} = sregulator:async_ask(Regulator, Ref),
    1 = sregulator:size(Regulator, ?TIMEOUT),
    ok.

await_timeout(_) ->
    {ok, Regulator} = sregulator_test:start_link(),
    {go, _, Regulator, 0} = sregulator:ask(Regulator),
    Ref = make_ref(),
    {await, Ref, Regulator} = sregulator:async_ask(Regulator, Ref),
    {'EXIT',
     {timeout,
      {sregulator, await, [Ref, 0]}}} = (catch sregulator:await(Ref, 0)),
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
      {sregulator, await, [Ref, 100]}}} = (catch sregulator:await(Ref, 100)),
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
