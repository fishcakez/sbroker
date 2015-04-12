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
%% @private
-module(sbroker_util).

-compile({no_auto_import, whereis/1}).

-export([whereis/1]).
-export([send_event/2]).
-export([sync_send_event/2]).
-export([sync_send_event/3]).
-export([async_send_event/2]).
-export([async_send_event/3]).

-type process() :: pid() | atom() | {atom(), node()} | {global, any()} |
    {via, module(), any()}.

-spec whereis(Process) -> Pid | {Name, Node} | undefined when
      Process :: process(),
      Pid :: pid(),
      Name :: atom(),
      Node :: node().
whereis(Pid) when is_pid(Pid) ->
    Pid;
whereis(Name) when is_atom(Name) ->
    erlang:whereis(Name);
whereis({Name, Node}) when is_atom(Name) andalso Node =:= node() ->
    erlang:whereis(Name);
whereis({Name, Node} = Process) when is_atom(Name) andalso is_atom(Node) ->
    Process;
whereis({global, Name}) ->
    global:whereis_name(Name);
whereis({via, Mod, Name}) ->
    Mod:whereis_name(Name).

-spec send_event(Process, Request) -> ok when
      Process :: process(),
      Request :: any().
send_event(Process, Request) ->
    MFA = {?MODULE, send_event, [Process, Request]},
    case whereis(Process) of
        Pid when is_pid(Pid) andalso node(Pid) =:= node() ->
            Start = sbroker_time:native(),
            gen_fsm:send_event(Pid, {Request, Start});
        Pid when is_pid(Pid) ->
            exit({{badnode, node(Pid)}, MFA});
        {Name, Node} when is_atom(Name) andalso is_atom(Node) ->
            exit({{badnode, Node}, MFA});
        undefined ->
            exit({noproc, MFA})
    end.

-spec sync_send_event(Process, Request) -> Response when
      Process :: process(),
      Request :: any(),
      Response :: any().
sync_send_event(Process, Request) ->
    MFA = {?MODULE, sync_send_event, [Process, Request]},
    case whereis(Process) of
        Pid when is_pid(Pid) andalso node(Pid) =:= node() ->
            Start = sbroker_time:native(),
            try
                gen_fsm:sync_send_event(Process, {Request, Start}, infinity)
            catch
                exit:{Reason, {gen_fsm, sync_send_event, _}} ->
                    exit({Reason, MFA})
            end;
        Pid when is_pid(Pid) ->
            exit({{badnode, node(Pid)}, MFA});
        {Name, Node} when is_atom(Name) andalso is_atom(Node) ->
            exit({{badnode, Node}, MFA});
        undefined ->
            exit({noproc, MFA})
    end.

-spec sync_send_event(Process, Request, Timeout) -> Response when
      Process :: process(),
      Request :: any(),
      Timeout :: timeout(),
      Response :: any().
sync_send_event(Process, Request, Timeout) ->
    MFA = {?MODULE, sync_send_event, [Process, Request, Timeout]},
    case whereis(Process) of
        Pid when is_pid(Pid) andalso node(Pid) =:= node() ->
            Start = sbroker_time:native(),
            try
                gen_fsm:sync_send_event(Process, {Request, Start}, Timeout)
            catch
                exit:{Reason, {gen_fsm, sync_send_event, _}} ->
                    exit({Reason, MFA})
            end;
        Pid when is_pid(Pid) ->
            exit({{badnode, node(Pid)}, MFA});
        {Name, Node} when is_atom(Name) andalso is_atom(Node) ->
            exit({{badnode, Node}, MFA});
        undefined ->
            exit({noproc, MFA})
    end.

-spec async_send_event(Process, Request) -> {await, MRef, Pid} when
      Process :: process(),
      Request :: any(),
      MRef :: reference(),
      Pid :: pid().
async_send_event(Process, Request) ->
    MFA = {?MODULE, async_send_event, [Process, Request]},
    case whereis(Process) of
        Pid when is_pid(Pid) andalso node(Pid) =:= node() ->
            MRef = monitor(process, Pid),
            Start = sbroker_time:native(),
            gen_fsm:send_event(Pid, {Request, Start, {self(), MRef}}),
            {await, MRef, Pid};
        Pid when is_pid(Pid) ->
            exit({{badnode, node(Pid)}, MFA});
        {Name, Node} when is_atom(Name) andalso is_atom(Node) ->
            exit({{badnode, Node}, MFA});
        undefined ->
            exit({noproc, MFA})
    end.

-spec async_send_event(Process, Request, Tag) -> {await, Tag, Pid} when
      Process :: process(),
      Request :: any(),
      Pid :: pid().
async_send_event(Process, Request, Tag) ->
    MFA = {?MODULE, async_send_event, [Process, Request, Tag]},
    case whereis(Process) of
        Pid when is_pid(Pid) andalso node(Pid) =:= node() ->
            Start = sbroker_time:native(),
            gen_fsm:send_event(Pid, {Request, Start, {self(), Tag}}),
            {await, Tag, Pid};
        Pid when is_pid(Pid) ->
            exit({{badnode, node(Pid)}, MFA});
        {Name, Node} when is_atom(Name) andalso is_atom(Node) ->
            exit({{badnode, Node}, MFA});
        undefined ->
            exit({noproc, MFA})
    end.
