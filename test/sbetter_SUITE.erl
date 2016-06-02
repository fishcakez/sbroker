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
-module(sbetter_SUITE).

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
-export([whereis_empty/1]).

-export([send_pid/1]).
-export([send_local/1]).
-export([send_global/1]).
-export([send_via/1]).
-export([send_empty/1]).

%% common_test api

all() ->
    [{group, whereis},
     {group, send}].

suite() ->
    [{timetrap, {seconds, 120}}].

groups() ->
    [{whereis, [parallel], [whereis_pid, whereis_local, whereis_global,
                            whereis_via, whereis_empty]},
     {send, [parallel], [send_pid, send_local, send_global, send_via,
                         send_empty]}].

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
    A = better_pid(),
    B = better_pid(),

    B = sbetter:whereis_name({{B}, scheduler_ask_r}),

    true = sbetter_server:update(A, 0, 1),
    true = sbetter_server:update(B, 1, 0),

    A = sbetter:whereis_name({{A, B}, ask}),
    B = sbetter:whereis_name({{A, B}, ask_r}),

    A = sbetter:whereis_name({{A, A, B}, ask}),

    ok.

whereis_local(_) ->
    A = better_pid(),
    B = better_pid(),

    true = sbetter_server:update(A, 0, 1),
    true = sbetter_server:update(B, 1, 0),

    erlang:register(?MODULE, A),

    A = sbetter:whereis_name({{?MODULE}, scheduler_ask}),
    A = sbetter:whereis_name({{?MODULE, B}, ask}),
    A = sbetter:whereis_name({{?MODULE, ?MODULE, B}, ask}),

    A = sbetter:whereis_name({{{?MODULE, node()}}, ask_r}),
    B = sbetter:whereis_name({{{?MODULE, node()}, B}, ask_r}),
    B = sbetter:whereis_name({{{?MODULE, node()}, B, B}, ask_r}),
    {'EXIT', {badnode, node}} = (catch sbetter:whereis_name({{{?MODULE,
                                                               node}}, ask})),

    erlang:unregister(?MODULE),

    undefined = sbetter:whereis_name({{?MODULE}, ask}),
    undefined = sbetter:whereis_name({{?MODULE, ?MODULE}, scheduler_ask}),
    undefined = sbetter:whereis_name({{?MODULE, ?MODULE, ?MODULE}, ask}),

    undefined = sbetter:whereis_name({{?MODULE, node()}, ask}),
    undefined = sbetter:whereis_name({{{?MODULE, node()},
                                       {?MODULE, node()}}, scheduler_ask_r}),
    undefined = sbetter:whereis_name({{{?MODULE, node()},
                                       {?MODULE, node()},
                                       {?MODULE, node()}}, ask}),

    ok.

whereis_global(_) ->
    A = better_pid(),
    B = better_pid(),

    true = sbetter_server:update(A, 0, 1),
    true = sbetter_server:update(B, 1, 0),

    yes = global:register_name({?MODULE, global}, A),
    Name = {global, {?MODULE, global}},

    A = sbetter:whereis_name({{Name}, ask}),
    B = sbetter:whereis_name({{Name, B}, ask_r}),
    A = sbetter:whereis_name({{Name, Name, A}, ask}),

    global:unregister_name({?MODULE, global}),

    undefined = sbetter:whereis_name({{Name}, ask}),
    undefined = sbetter:whereis_name({{Name, Name}, scheduler_ask}),
    undefined = sbetter:whereis_name({{Name, Name, Name}, ask_r}),

    ok.

whereis_via(_) ->
    A = better_pid(),
    B = better_pid(),

    true = sbetter_server:update(A, 0, 1),
    true = sbetter_server:update(B, 1, 0),

    yes = global:register_name({?MODULE, via}, A),
    Name = {via, global, {?MODULE, via}},

    A = sbetter:whereis_name({{Name}, ask}),
    B = sbetter:whereis_name({{Name, B}, ask_r}),
    A = sbetter:whereis_name({{Name, Name, A}, ask}),

    global:unregister_name({?MODULE, via}),

    undefined = sbetter:whereis_name({{Name}, scheduler_ask_r}),
    undefined = sbetter:whereis_name({{Name, Name}, ask}),
    undefined = sbetter:whereis_name({{Name, Name, Name}, scheduler_ask}),

    ok.

whereis_empty(_) ->
    undefined = sbetter:whereis_name({{}, ask_r}),

    ok.

send_pid(_) ->
    Self = self(),
    sbetter_server:register(Self, 1, 1),
    Ref = make_ref(),

    sbetter:send({{Self}, ask}, Ref),
    receive Ref -> ok after 100 -> exit(no_msg) end,

    sbetter:send({{Self, Self}, ask_r}, Ref),
    receive Ref -> ok after 100 -> exit(no_msg) end,

    ok.

send_local(_) ->
    Self = self(),
    erlang:register(?MODULE, Self),
    sbetter_server:register(Self, 1, 1),
    Ref = make_ref(),

    sbetter:send({{?MODULE}, ask}, Ref),
    receive Ref -> ok after 100 -> exit(no_msg) end,

    sbetter:send({{?MODULE, ?MODULE}, scheduler_ask_r}, Ref),
    receive Ref -> ok after 100 -> exit(no_msg) end,

    sbetter:send({{{?MODULE, node()}}, scheduler_ask}, Ref),
    receive Ref -> ok after 100 -> exit(no_msg) end,

    sbetter:send({{{?MODULE, node()}, {?MODULE, node()}}, ask_r}, Ref),
    receive Ref -> ok after 100 -> exit(no_msg) end,

    erlang:unregister(?MODULE),

    try sbetter:send({{?MODULE}, ask}, Ref) of
        _ ->
            exit(no_exit)
    catch
        exit:{noproc, {sbetter, send, [{{?MODULE}, ask}, Ref]}} ->
            ok
    end,

    try sbetter:send({{?MODULE, ?MODULE}, scheduler_ask}, Ref) of
        _ ->
            exit(no_exit)
    catch
        exit:{noproc, {sbetter, send, [{{?MODULE, ?MODULE}, scheduler_ask},
                                       Ref]}} ->
            ok
    end,

    try sbetter:send({{{?MODULE, node()}}, scheduler_ask_r}, Ref) of
        _ ->
            exit(no_exit)
    catch
        exit:{noproc, {sbetter, send, [{{{?MODULE, Node}}, scheduler_ask_r},
                                       Ref]}}
          when Node =:= node() ->
            ok
    end,

    try sbetter:send({{{?MODULE, node()}, {?MODULE, node()}}, ask_r}, Ref) of
        _ ->
            exit(no_exit)
    catch
        exit:{noproc,
              {sbetter, send,
               [{{{?MODULE, Node2}, {?MODULE, Node2}}, ask_r}, Ref]}}
          when Node2 =:= node() ->
            ok
    end,


    ok.

send_global(_) ->
    Self = self(),
    yes = global:register_name({?MODULE, global}, Self),
    sbetter_server:register(Self, 1, 1),
    Name = {global, {?MODULE, global}},
    Ref = make_ref(),

    sbetter:send({{Name}, scheduler_ask}, Ref),
    receive Ref -> ok after 100 -> exit(no_msg) end,

    sbetter:send({{Name, Name}, ask_r}, Ref),
    receive Ref -> ok after 100 -> exit(no_msg) end,

    global:unregister_name({?MODULE, global}),

    try sbetter:send({{Name}, ask}, Ref) of
        _ ->
            exit(no_exit)
    catch
        exit:{noproc, {sbetter, send, [{{Name}, ask}, Ref]}} ->
            ok
    end,

    try sbetter:send({{Name, Name}, scheduler_ask_r}, Ref) of
        _ ->
            exit(no_exit)
    catch
        exit:{noproc, {sbetter, send, [{{Name, Name}, scheduler_ask_r},
                                       Ref]}} ->
            ok
    end,

    try sbetter:send({{Name, Name, Name}, ask_r}, Ref) of
        _ ->
            exit(no_exit)
    catch
        exit:{noproc, {sbetter, send, [{{Name, Name, Name}, ask_r},
                                       Ref]}} ->
            ok
    end,

    ok.

send_via(_) ->
    Self = self(),
    yes = global:register_name({?MODULE, via}, Self),
    sbetter_server:register(Self, 1, 1),
    Name = {via, global, {?MODULE, via}},
    Ref = make_ref(),

    sbetter:send({{Name}, scheduler_ask}, Ref),
    receive Ref -> ok after 100 -> exit(no_msg) end,

    sbetter:send({{Name, Name}, ask_r}, Ref),
    receive Ref -> ok after 100 -> exit(no_msg) end,

    global:unregister_name({?MODULE, via}),

    try sbetter:send({{Name}, scheduler_ask_r}, Ref) of
        _ ->
            exit(no_exit)
    catch
        exit:{noproc, {sbetter, send, [{{Name}, scheduler_ask_r}, Ref]}} ->
            ok
    end,

    try sbetter:send({{Name, Name}, ask}, Ref) of
        _ ->
            exit(no_exit)
    catch
        exit:{noproc, {sbetter, send, [{{Name, Name}, ask}, Ref]}} ->
            ok
    end,

    try sbetter:send({{Name, Name, Name}, scheduler_ask}, Ref) of
        _ ->
            exit(no_exit)
    catch
        exit:{noproc, {sbetter, send, [{{Name, Name, Name}, scheduler_ask},
                                       Ref]}} ->
            ok
    end,

    ok.

send_empty(_) ->
    Ref = make_ref(),

    try sbetter:send({{}, ask}, Ref) of
        _ ->
            exit(no_exit)
    catch
        exit:{noproc, {sbetter, send, [{{}, ask}, Ref]}} ->
            ok
    end,

    try sbetter:send({{}, scheduler_ask_r}, Ref) of
        _ ->
            exit(no_exit)
    catch
        exit:{noproc, {sbetter, send, [{{}, scheduler_ask_r}, Ref]}} ->
            ok
    end,

    ok.

%% Internal

better_pid() ->
    Pid = spawn_link(timer, sleep, [infinity]),
    sbetter_server:register(Pid, 0, 0),
    Pid.
