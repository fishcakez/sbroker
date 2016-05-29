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
%% @doc This modules provides utility functions for basic load balancing based
%% on the scheduler id of the calling process. It is designed for use with OTP
%% behaviour messaging using `via' names, e.g.
%% `{via, sscheduler, {Process,...}}'. The third element, `{Process,...}', is a
%% tuple containing pids (`pid()') and/or process names (`atom()',
%% `{global, any()}', `{via, module(), any()}' or `{atom(), node()}'). An
%% element is chosen based on the scheduler id, if the element is a `pid()' it
%% is returned, otherwise the `pid()' of the process name is looked up.
%%
%% It is not possible to locally look up the pid of a process with name
%% `{atom(), node()}' if the node is not the local node. Therefore a registered
%% name on another node is not supported for use with this module.
-module(sscheduler).

-export([whereis_name/1]).
-export([send/2]).

%% @doc Lookup the pid or process name of one element, selected based on the
%% scheduler id, in a tuple of process names. If no process is associated with
%% the name returns `undefined'.
-spec whereis_name(Processes) -> Process | undefined when
      Processes :: tuple(),
      Process :: pid().
whereis_name({}) ->
    undefined;
whereis_name({Name}) ->
    case sbroker_gen:whereis(Name) of
        Pid when is_pid(Pid) ->
            Pid;
        undefined ->
            undefined;
        {_, Node} ->
            exit({badnode, Node})
    end;
whereis_name(Processes) when is_tuple(Processes) ->
    Size = tuple_size(Processes),
    Scheduler = erlang:system_info(scheduler_id),
    N = (Scheduler rem Size) + 1,
    Name = element(N, Processes),
    case sbroker_gen:whereis(Name) of
        Pid when is_pid(Pid) ->
            Pid;
        undefined ->
            undefined;
        {_, Node} ->
            exit({badnode, Node})
    end.

%% @doc Send a message to one element, selected based on the scheduler id, in a
%% tuple of process names. Returns `ok' if the element chosen is a `pid()'.
%% a locally registered name on another node, or a process is associated with
%% the name. Otherwise exits.
-spec send(Processes, Msg) -> ok when
      Processes :: tuple(),
      Msg :: any().
send(Processes, Msg) ->
    case whereis_name(Processes) of
        Pid when is_pid(Pid) ->
            _ = Pid ! Msg,
            ok;
        undefined ->
            exit({noproc, {?MODULE, send, [Processes, Msg]}})
    end.
