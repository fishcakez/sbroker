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
-module(sprotector_SUITE).

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

-export([whereis_pid/1]).
-export([whereis_local/1]).
-export([whereis_global/1]).
-export([whereis_via/1]).

-export([send_pid/1]).
-export([send_local/1]).
-export([send_global/1]).
-export([send_via/1]).

%% common_test api

all() ->
    [{group, whereis},
     {group, send}].

suite() ->
    [{timetrap, {seconds, 120}}].

groups() ->
    [{whereis, [parallel], [whereis_pid, whereis_local, whereis_global,
                            whereis_via]},
     {send, [parallel], [send_pid, send_local, send_global, send_via]}].

init_per_suite(Config) ->
    {ok, Started} = application:ensure_all_started(sbroker),
    [{started, Started} | Config].

end_per_suite(Config) ->
    Started = ?config(started, Config),
    _ = [application:stop(App) || App <- Started],
    ok.

group(_Group) ->
    [].

init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%% test cases

whereis_pid(_) ->
    Self = self(),
    true = sprotector_server:register(Self, 1, 2),

    Self = sprotector:whereis_name({Self, ask}),
    Self = sprotector:whereis_name({Self, ask}),
    {'EXIT', drop} = (catch sprotector:whereis_name({Self, ask_r})),

    true = sprotector_server:update(Self, 0, 1.0, 1.0),

    Self = sprotector:whereis_name({Self, ask}),
    {'EXIT', drop} = (catch sprotector:whereis_name({Self, ask})),
    {'EXIT', drop} = (catch sprotector:whereis_name({Self, ask_r})),

    ok.

whereis_local(_) ->
    Self = self(),
    true = sprotector_server:register(Self, 1, 1),

    erlang:register(?MODULE, Self),

    Self = sprotector:whereis_name({?MODULE, ask_r}),
    {'EXIT', drop} = (catch sprotector:whereis_name({?MODULE, ask})),

    {'EXIT', drop} = (catch sprotector:whereis_name({{?MODULE, node()},
                                                     ask_r})),

    {'EXIT', {badnode, node}} = (catch sprotector:whereis_name({{?MODULE, node},
                                                              ask})),

    erlang:unregister(?MODULE),

    undefined = sprotector:whereis_name({?MODULE, ask}),
    undefined = sprotector:whereis_name({{?MODULE, node()}, ask_r}),

    ok.

whereis_global(_) ->
    Self = self(),
    true = sprotector_server:register(Self, 1, 1),

    yes = global:register_name({?MODULE, global}, Self),
    Name = {global, {?MODULE, global}},

    Self = sprotector:whereis_name({Name, ask}),
    {'EXIT', drop} = (catch sprotector:whereis_name({Name, ask_r})),

    global:unregister_name({?MODULE, global}),

    undefined = sprotector:whereis_name({Name, ask}),
    undefined = sprotector:whereis_name({Name, ask_r}),

    ok.

whereis_via(_) ->
    Self = self(),
    true = sprotector_server:register(Self, 1, 1),

    yes = global:register_name({?MODULE, via}, Self),
    Name = {via, global, {?MODULE, via}},

    Self = sprotector:whereis_name({Name, ask}),
    {'EXIT', drop} = (catch sprotector:whereis_name({Name, ask_r})),

    global:unregister_name({?MODULE, via}),

    undefined = sprotector:whereis_name({Name, ask}),
    undefined = sprotector:whereis_name({Name, ask_r}),

    ok.

send_pid(_) ->
    Self = self(),
    true = sprotector_server:register(Self, 1, 1),
    Ref = make_ref(),

    sprotector:send({Self, ask}, Ref),
    receive Ref -> ok after 100 -> exit(no_msg) end,

    try sprotector:send({Self, ask_r}, Ref) of
        _ ->
            exit(no_exit)
    catch
        exit:{drop, {sprotector, send, [{Self, ask_r}, Ref]}} ->
            ok
    end,

    ok.

send_local(_) ->
    Self = self(),
    true = sprotector_server:register(Self, 1, 1),
    Ref = make_ref(),

    erlang:register(?MODULE, Self),

    sprotector:send({?MODULE, ask}, Ref),
    receive Ref -> ok after 100 -> exit(no_msg) end,

     try sprotector:send({?MODULE, ask_r}, Ref) of
        _ ->
            exit(no_exit)
    catch
        exit:{drop, {sprotector, send, [{?MODULE, ask_r}, Ref]}} ->
            ok
    end,

    try sprotector:send({{?MODULE, node}, ask_r}, Ref) of
        _ ->
            exit(no_exit)
    catch
        exit:{{badnode, node},
              {sprotector, send, [{{?MODULE, node}, ask_r}, Ref]}} ->
            ok
    end,

    erlang:unregister(?MODULE),

    try sprotector:send({?MODULE, ask}, Ref) of
        _ ->
            exit(no_exit)
    catch
        exit:{noproc, {sprotector, send, [{?MODULE, ask}, Ref]}} ->
            ok
    end,

    try sprotector:send({{?MODULE, node()}, ask_r}, Ref) of
        _ ->
            exit(no_exit)
    catch
        exit:{noproc, {sprotector, send, [{{?MODULE, Node}, ask_r}, Ref]}}
          when Node =:= node() ->
            ok
    end,

    ok.

send_global(_) ->
    Self = self(),
    yes = global:register_name({?MODULE, global}, Self),
    true = sprotector_server:register(Self, 1, 1),
    Name = {global, {?MODULE, global}},
    Ref = make_ref(),

    sprotector:send({Name, ask_r}, Ref),
    receive Ref -> ok after 100 -> exit(no_msg) end,

    try sprotector:send({Name, ask}, Ref) of
        _ ->
            exit(no_exit)
    catch
        exit:{drop, {sprotector, send, [{Name, ask}, Ref]}} ->
            ok
    end,

    global:unregister_name({?MODULE, global}),

    try sprotector:send({Name, ask}, Ref) of
        _ ->
            exit(no_exit)
    catch
        exit:{noproc, {sprotector, send, [{Name, ask}, Ref]}} ->
            ok
    end,

    ok.

send_via(_) ->
    Self = self(),
    yes = global:register_name({?MODULE, via}, Self),
    true = sprotector_server:register(Self, 1, 1),
    Name = {global, {?MODULE, via}},
    Ref = make_ref(),

    sprotector:send({Name, ask_r}, Ref),
    receive Ref -> ok after 100 -> exit(no_msg) end,

    try sprotector:send({Name, ask}, Ref) of
        _ ->
            exit(no_exit)
    catch
        exit:{drop, {sprotector, send, [{Name, ask}, Ref]}} ->
            ok
    end,

    global:unregister_name({?MODULE, via}),

    try sprotector:send({Name, ask}, Ref) of
        _ ->
            exit(no_exit)
    catch
        exit:{noproc, {sprotector, send, [{Name, ask}, Ref]}} ->
            ok
    end,

    ok.
