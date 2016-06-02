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
-module(sbroker_via_SUITE).

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
    [{group, srand},
     {group, sscheduler}].

suite() ->
    [{timetrap, {seconds, 120}}].

groups() ->
    [{whereis, [parallel], [whereis_pid, whereis_local, whereis_global,
                            whereis_via, whereis_empty]},
     {send, [parallel], [send_pid, send_local, send_global, send_via,
                         send_empty]},
     {srand, [{group, whereis}, {group, send}]},
     {sscheduler, [{group, whereis}, {group, send}]}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

group(_Group) ->
    [].

init_per_group(Mod, Config) when Mod == srand; Mod == sscheduler ->
    [{via, Mod} | Config];
init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%% test cases

whereis_pid(Config) ->
    Via = ?config(via, Config),
    Self = self(),

    Self = Via:whereis_name({Self}),
    Self = Via:whereis_name({Self, Self}),

    ok.

whereis_local(Config) ->
    Via = ?config(via, Config),
    Self = self(),
    erlang:register(?MODULE, Self),

    Self = Via:whereis_name({?MODULE}),
    Self = Via:whereis_name({?MODULE, ?MODULE}),

    Self = Via:whereis_name({{?MODULE, node()}}),
    Self = Via:whereis_name({{?MODULE, node()}, {?MODULE, node()}}),

    {'EXIT', {badnode, node}} = (catch Via:whereis_name({{?MODULE,
                                                                 node}})),

    erlang:unregister(?MODULE),

    undefined = Via:whereis_name({?MODULE}),
    undefined = Via:whereis_name({?MODULE, ?MODULE}),

    undefined = Via:whereis_name({{?MODULE, node()}}),
    undefined = Via:whereis_name({{?MODULE, node()},
                                          {?MODULE, node()}}),

    ok.

whereis_global(Config) ->
    Via = ?config(via, Config),
    Self = self(),
    yes = global:register_name({?MODULE, global}, Self),
    Name = {global, {?MODULE, global}},

    Self = Via:whereis_name({Name}),
    Self = Via:whereis_name({Name, Name}),

    global:unregister_name({?MODULE, global}),

    undefined = Via:whereis_name({Name}),
    undefined = Via:whereis_name({Name, Name}),

    ok.

whereis_via(Config) ->
    Via = ?config(via, Config),
    Self = self(),
    yes = global:register_name({?MODULE, via}, Self),
    Name = {via, global, {?MODULE, via}},

    Self = Via:whereis_name({Name}),
    Self = Via:whereis_name({Name, Name}),

    global:unregister_name({?MODULE, via}),

    undefined = Via:whereis_name({Name}),
    undefined = Via:whereis_name({Name, Name}),

    ok.

whereis_empty(Config) ->
    Via = ?config(via, Config),
    undefined = Via:whereis_name({}),

    ok.

send_pid(Config) ->
    Via = ?config(via, Config),
    Self = self(),
    Ref = make_ref(),

    Via:send({Self}, Ref),
    receive Ref -> ok after 100 -> exit(no_msg) end,

    Via:send({Self, Self}, Ref),
    receive Ref -> ok after 100 -> exit(no_msg) end,

    ok.

send_local(Config) ->
    Via = ?config(via, Config),
    erlang:register(?MODULE, self()),
    Ref = make_ref(),

    Via:send({?MODULE}, Ref),
    receive Ref -> ok after 100 -> exit(no_msg) end,

    Via:send({?MODULE, ?MODULE}, Ref),
    receive Ref -> ok after 100 -> exit(no_msg) end,

    Self = Via:send({{?MODULE, node()}}, Ref),
    receive Ref -> ok after 100 -> exit(no_msg) end,

    Self = Via:send({{?MODULE, node()}, {?MODULE, node()}}, Ref),
    receive Ref -> ok after 100 -> exit(no_msg) end,

    erlang:unregister(?MODULE),

    try Via:send({?MODULE}, Ref) of
        _ ->
            exit(no_exit)
    catch
        exit:{noproc, {Via, send, [{?MODULE}, Ref]}} ->
            ok
    end,

    try Via:send({?MODULE, ?MODULE}, Ref) of
        _ ->
            exit(no_exit)
    catch
        exit:{noproc, {Via, send, [{?MODULE, ?MODULE}, Ref]}} ->
            ok
    end,

    try Via:send({{?MODULE, node()}}, Ref) of
        _ ->
            exit(no_exit)
    catch
        exit:{noproc, {Via, send, [{{?MODULE, Node}}, Ref]}}
          when Node =:= node() ->
            ok
    end,

    try Via:send({{?MODULE, node()}, {?MODULE, node()}}, Ref) of
        _ ->
            exit(no_exit)
    catch
        exit:{noproc,
              {Via, send,
               [{{?MODULE, Node2}, {?MODULE, Node2}}, Ref]}}
          when Node2 =:= node() ->
            ok
    end,


    ok.

send_global(Config) ->
    Via = ?config(via, Config),
    yes = global:register_name({?MODULE, global}, self()),
    Name = {global, {?MODULE, global}},
    Ref = make_ref(),

    Via:send({Name}, Ref),
    receive Ref -> ok after 100 -> exit(no_msg) end,

    Via:send({Name, Name}, Ref),
    receive Ref -> ok after 100 -> exit(no_msg) end,

    global:unregister_name({?MODULE, global}),

    try Via:send({Name}, Ref) of
        _ ->
            exit(no_exit)
    catch
        exit:{noproc, {Via, send, [{Name}, Ref]}} ->
            ok
    end,

    try Via:send({Name, Name}, Ref) of
        _ ->
            exit(no_exit)
    catch
        exit:{noproc, {Via, send, [{Name, Name}, Ref]}} ->
            ok
    end,

    ok.

send_via(Config) ->
    Via = ?config(via, Config),
    yes = global:register_name({?MODULE, via}, self()),
    Name = {via, global, {?MODULE, via}},
    Ref = make_ref(),

    Via:send({Name}, Ref),
    receive Ref -> ok after 100 -> exit(no_msg) end,

    Via:send({Name, Name}, Ref),
    receive Ref -> ok after 100 -> exit(no_msg) end,

    global:unregister_name({?MODULE, via}),

    try Via:send({Name}, Ref) of
        _ ->
            exit(no_exit)
    catch
        exit:{noproc, {Via, send, [{Name}, Ref]}} ->
            ok
    end,

    try Via:send({Name, Name}, Ref) of
        _ ->
            exit(no_exit)
    catch
        exit:{noproc, {Via, send, [{Name, Name}, Ref]}} ->
            ok
    end,

    ok.

send_empty(Config) ->
    Via = ?config(via, Config),
    Ref = make_ref(),

    try Via:send({}, Ref) of
        _ ->
            exit(no_exit)
    catch
        exit:{noproc, {Via, send, [{}, Ref]}} ->
            ok
    end,

    ok.
