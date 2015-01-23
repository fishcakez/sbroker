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

-export([statem/1]).
-export([ask_whereis_name/1]).
-export([ask_send/1]).
-export([nb_ask_whereis_name/1]).
-export([nb_ask_send/1]).
-export([ask_r_whereis_name/1]).
-export([ask_r_send/1]).
-export([nb_ask_r_whereis_name/1]).
-export([nb_ask_r_send/1]).

%% common_test api

all() ->
    [{group, property},
     {group, via}].

suite() ->
    [{ct_hooks, [cth_surefire]},
     {timetrap, {seconds, 120}}].

groups() ->
    [{property, [statem]},
     {via, [parallel], [ask_whereis_name, ask_send, nb_ask_whereis_name,
                        nb_ask_send, ask_r_whereis_name, ask_r_send,
                        nb_ask_r_whereis_name, nb_ask_r_send]}].

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

ask_whereis_name(_) ->
    {ok, Broker} = sbroker_test:start_link(),
    Ref = make_ref(),
    Self = self(),

    {await, Ref, Broker} = sbroker:async_ask_r(Broker, Ref),
    Self = sbroker_ask:whereis_name(Broker),
    receive {Ref, {go, _, Self, _}} -> ok after 100 -> exit(timeout) end,

    undefined = sbroker_ask:whereis_name(Broker),

    ok.

ask_send(_) ->
    {ok, Broker} = sbroker_test:start_link(),
    Ref = make_ref(),
    Self = self(),

    {await, Ref, Broker} = sbroker:async_ask_r(Broker, Ref),
    ok = sbroker_ask:send(Broker, hello),
    receive {Ref, {go, _, Self, _}} -> ok after 100 -> exit(timeout) end,
    receive hello -> ok after 100 -> exit(timeout) end,

    try sbroker_ask:send(Broker, hello) of
        ok ->
            exit(no_exit)
    catch
        exit:{noproc, {sbroker_ask, send, [Broker, hello]}} ->
            ok
    end,

    ok.

nb_ask_whereis_name(_) ->
    {ok, Broker} = sbroker_test:start_link(),
    Ref = make_ref(),
    Self = self(),

    {await, Ref, Broker} = sbroker:async_ask_r(Broker, Ref),
    Self = sbroker_nb_ask:whereis_name(Broker),
    receive {Ref, {go, _, Self, _}} -> ok after 100 -> exit(timeout) end,

    undefined = sbroker_nb_ask:whereis_name(Broker),

    ok.

nb_ask_send(_) ->
    {ok, Broker} = sbroker_test:start_link(),
    Ref = make_ref(),
    Self = self(),

    {await, Ref, Broker} = sbroker:async_ask_r(Broker, Ref),
    ok = sbroker_nb_ask:send(Broker, hello),
    receive {Ref, {go, _, Self, _}} -> ok after 100 -> exit(timeout) end,
    receive hello -> ok after 100 -> exit(timeout) end,

    try sbroker_nb_ask:send(Broker, hello) of
        ok ->
            exit(no_exit)
    catch
        exit:{noproc, {sbroker_nb_ask, send, [Broker, hello]}} ->
            ok
    end,

    ok.

ask_r_whereis_name(_) ->
    {ok, Broker} = sbroker_test:start_link(),
    Ref = make_ref(),
    Self = self(),

    {await, Ref, Broker} = sbroker:async_ask(Broker, Ref),
    Self = sbroker_ask_r:whereis_name(Broker),
    receive {Ref, {go, _, Self, _}} -> ok after 100 -> exit(timeout) end,

    undefined = sbroker_ask_r:whereis_name(Broker),

    ok.

ask_r_send(_) ->
    {ok, Broker} = sbroker_test:start_link(),
    Ref = make_ref(),
    Self = self(),

    {await, Ref, Broker} = sbroker:async_ask(Broker, Ref),
    ok = sbroker_ask_r:send(Broker, hello),
    receive {Ref, {go, _, Self, _}} -> ok after 100 -> exit(timeout) end,
    receive hello -> ok after 100 -> exit(timeout) end,

    try sbroker_ask_r:send(Broker, hello) of
        ok ->
            exit(no_exit)
    catch
        exit:{noproc, {sbroker_ask_r, send, [Broker, hello]}} ->
            ok
    end,

    ok.

nb_ask_r_whereis_name(_) ->
    {ok, Broker} = sbroker_test:start_link(),
    Ref = make_ref(),
    Self = self(),

    {await, Ref, Broker} = sbroker:async_ask(Broker, Ref),
    Self = sbroker_nb_ask_r:whereis_name(Broker),
    receive {Ref, {go, _, Self, _}} -> ok after 100 -> exit(timeout) end,

    undefined = sbroker_nb_ask_r:whereis_name(Broker),

    ok.

nb_ask_r_send(_) ->
    {ok, Broker} = sbroker_test:start_link(),
    Ref = make_ref(),
    Self = self(),

    {await, Ref, Broker} = sbroker:async_ask(Broker, Ref),
    ok = sbroker_nb_ask_r:send(Broker, hello),
    receive {Ref, {go, _, Self, _}} -> ok after 100 -> exit(timeout) end,
    receive hello -> ok after 100 -> exit(timeout) end,

    try sbroker_nb_ask_r:send(Broker, hello) of
        ok ->
            exit(no_exit)
    catch
        exit:{noproc, {sbroker_nb_ask_r, send, [Broker, hello]}} ->
            ok
    end,

    ok.

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
