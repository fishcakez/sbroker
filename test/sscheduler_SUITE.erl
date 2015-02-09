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
-module(sscheduler_SUITE).

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
    [{ct_hooks, [cth_surefire]},
     {timetrap, {seconds, 120}}].

groups() ->
    [{whereis, [parallel], [whereis_pid, whereis_local, whereis_global,
                            whereis_via, whereis_empty]},
     {send, [parallel], [send_pid, send_local, send_global, send_via,
                         send_empty]}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
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

    Self = sscheduler:whereis_name({Self}),
    Self = sscheduler:whereis_name({Self, Self}),

    ok.

whereis_local(_) ->
    Self = self(),
    erlang:register(?MODULE, Self),

    Self = sscheduler:whereis_name({?MODULE}),
    Self = sscheduler:whereis_name({?MODULE, ?MODULE}),

    Self = sscheduler:whereis_name({{?MODULE, node()}}),
    Self = sscheduler:whereis_name({{?MODULE, node()}, {?MODULE, node()}}),

    {?MODULE, node} = sscheduler:whereis_name({{?MODULE, node}}),
    {?MODULE, node} = sscheduler:whereis_name({{?MODULE, node},
                                               {?MODULE, node}}),

    erlang:unregister(?MODULE),

    undefined = sscheduler:whereis_name({?MODULE}),
    undefined = sscheduler:whereis_name({?MODULE, ?MODULE}),

    undefined = sscheduler:whereis_name({{?MODULE, node()}}),
    undefined = sscheduler:whereis_name({{?MODULE, node()},
                                          {?MODULE, node()}}),

    ok.

whereis_global(_) ->
    Self = self(),
    yes = global:register_name({?MODULE, global}, Self),
    Name = {global, {?MODULE, global}},

    Self = sscheduler:whereis_name({Name}),
    Self = sscheduler:whereis_name({Name, Name}),

    global:unregister_name({?MODULE, global}),

    undefined = sscheduler:whereis_name({Name}),
    undefined = sscheduler:whereis_name({Name, Name}),

    ok.

whereis_via(_) ->
    Self = self(),
    yes = global:register_name({?MODULE, via}, Self),
    Name = {via, global, {?MODULE, via}},

    Self = sscheduler:whereis_name({Name}),
    Self = sscheduler:whereis_name({Name, Name}),

    global:unregister_name({?MODULE, via}),

    undefined = sscheduler:whereis_name({Name}),
    undefined = sscheduler:whereis_name({Name, Name}),

    ok.

whereis_empty(_) ->
    undefined = sscheduler:whereis_name({}),

    ok.

send_pid(_) ->
    Self = self(),
    Ref = make_ref(),

    sscheduler:send({Self}, Ref),
    receive Ref -> ok after 100 -> exit(no_msg) end,

    sscheduler:send({Self, Self}, Ref),
    receive Ref -> ok after 100 -> exit(no_msg) end,

    ok.

send_local(_) ->
    erlang:register(?MODULE, self()),
    Ref = make_ref(),

    sscheduler:send({?MODULE}, Ref),
    receive Ref -> ok after 100 -> exit(no_msg) end,

    sscheduler:send({?MODULE, ?MODULE}, Ref),
    receive Ref -> ok after 100 -> exit(no_msg) end,

    Self = sscheduler:send({{?MODULE, node()}}, Ref),
    receive Ref -> ok after 100 -> exit(no_msg) end,

    Self = sscheduler:send({{?MODULE, node()}, {?MODULE, node()}}, Ref),
    receive Ref -> ok after 100 -> exit(no_msg) end,

    sscheduler:send({{?MODULE, node}}, Ref),
    sscheduler:send({{?MODULE, node}, {?MODULE, node}}, Ref),

    erlang:unregister(?MODULE),

    try sscheduler:send({?MODULE}, Ref) of
        _ ->
            exit(no_exit)
    catch
        exit:{noproc, {sscheduler, send, [{?MODULE}, Ref]}} ->
            ok
    end,

    try sscheduler:send({?MODULE, ?MODULE}, Ref) of
        _ ->
            exit(no_exit)
    catch
        exit:{noproc, {sscheduler, send, [{?MODULE, ?MODULE}, Ref]}} ->
            ok
    end,

    try sscheduler:send({{?MODULE, node()}}, Ref) of
        _ ->
            exit(no_exit)
    catch
        exit:{noproc, {sscheduler, send, [{{?MODULE, Node}}, Ref]}}
          when Node =:= node() ->
            ok
    end,

    try sscheduler:send({{?MODULE, node()}, {?MODULE, node()}}, Ref) of
        _ ->
            exit(no_exit)
    catch
        exit:{noproc,
              {sscheduler, send,
               [{{?MODULE, Node2}, {?MODULE, Node2}}, Ref]}}
          when Node2 =:= node() ->
            ok
    end,


    ok.

send_global(_) ->
    yes = global:register_name({?MODULE, global}, self()),
    Name = {global, {?MODULE, global}},
    Ref = make_ref(),

    sscheduler:send({Name}, Ref),
    receive Ref -> ok after 100 -> exit(no_msg) end,

    sscheduler:send({Name, Name}, Ref),
    receive Ref -> ok after 100 -> exit(no_msg) end,

    global:unregister_name({?MODULE, global}),

    try sscheduler:send({Name}, Ref) of
        _ ->
            exit(no_exit)
    catch
        exit:{noproc, {sscheduler, send, [{Name}, Ref]}} ->
            ok
    end,

    try sscheduler:send({Name, Name}, Ref) of
        _ ->
            exit(no_exit)
    catch
        exit:{noproc, {sscheduler, send, [{Name, Name}, Ref]}} ->
            ok
    end,

    ok.

send_via(_) ->
    yes = global:register_name({?MODULE, via}, self()),
    Name = {via, global, {?MODULE, via}},
    Ref = make_ref(),

    sscheduler:send({Name}, Ref),
    receive Ref -> ok after 100 -> exit(no_msg) end,

    sscheduler:send({Name, Name}, Ref),
    receive Ref -> ok after 100 -> exit(no_msg) end,

    global:unregister_name({?MODULE, via}),

    try sscheduler:send({Name}, Ref) of
        _ ->
            exit(no_exit)
    catch
        exit:{noproc, {sscheduler, send, [{Name}, Ref]}} ->
            ok
    end,

    try sscheduler:send({Name, Name}, Ref) of
        _ ->
            exit(no_exit)
    catch
        exit:{noproc, {sscheduler, send, [{Name, Name}, Ref]}} ->
            ok
    end,

    ok.

send_empty(_) ->
    Ref = make_ref(),

    try sscheduler:send({}, Ref) of
        _ ->
            exit(no_exit)
    catch
        exit:{noproc, {sscheduler, send, [{}, Ref]}} ->
            ok
    end,

    ok.
