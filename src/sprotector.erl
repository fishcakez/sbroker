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
%% @doc This modules provides utility functions for overload protection and/or
%% short circuiting calls to a process. It is designed for use with `sbroker'
%% and `sregulator' processes using the `sprotector_pie_meter' meter. However
%% any OTP process can use this module to do load balancing using the `via'
%% naming format if the process is registered with and updates the
%% `sprotector_server'.
%%
%% To use `sprotector' with `via' use names of the form
%% `{via, sprotector, {Broker, ask | ask_r}}'. Where `{Broker, ...}' is
%% a tuple containing
%% `pid() | atom() | {global, any()} | {via, module(), any()} | {atom(), node()}'.
%% The lookup will succeed if the approximate queue length is less than or equal
%% to the minimum queue length or below the the maximum and the drop probability
%% allows. Otherwise the call exits with reason `drop'.
%%
%% Therefore when combining with `sbetter' use format:
%% `{via, sprotector, {{via, sbetter, {..., ask | ask_r}, ask | ask_r}}'. So the
%% load balancing lookup is resolved first and wrapped by `sprotector'.
%%
%% It is not possible to locally look up the pid of a process with name
%% `{atom(), node()}' if the node is not the local node. Therefore a registered
%% name on another node is not supported for use with this module.
%%
%% If a chosen process is not local the call may exit with `{badnode, node()}'.
%%
%% If a chosen process is not registered with the `sprotector_server' the call
%% may exit with `{noprotector, pid()}'. The `sdropper_pie_meter' will register
%% with the server and update it using the PIE active queue management
%% algorithm. However other methods can be used to register and update the
%% `sprotector_server'. Registering with the `sdropper_server' must be done with
%% `sprotector_server:register/1' and not using
%% `start_link({via, sprotector, ...}, ...)'.
%%
%% @see sprotector_pie_meter
%% @see sprotector_server
-module(sprotector).

%% public API

-export([whereis_name/1]).
-export([send/2]).

%% types

-type process() :: pid() | atom() | {atom(), node()} | {global, term()} |
                   {via, module(), term()}.

%% @doc Lookup a pid and possibly drop the request depending on the min, max and
%% drop probability of the chosen queue. If no process is associated with the
%% process returns `undefined'.
-spec whereis_name({Process, ask | ask_r}) -> Pid | undefined when
      Process :: process(),
      Pid :: pid().
whereis_name({Process, Key}) ->
    case info(Process, Key) of
        {go, Pid} ->
            Pid;
        {drop, _} ->
            exit(drop);
        undefined ->
            undefined;
        Error ->
            exit(Error)
    end.

%% @doc Sends a message to a pid if not dropped by the min, max and drop
%% probability of the chosen queue. Returns `ok' if the message is sent
%% otherwise exits.
-spec send({Process, ask | ask_r}, Msg) -> ok when
      Process :: process(),
      Msg :: any().
send(Process, Msg) ->
    try whereis_name(Process) of
        Pid when is_pid(Pid) ->
            _ = Pid ! Msg,
            ok;
        undefined ->
            exit({noproc, {?MODULE, send, [Process, Msg]}})
    catch
        exit:Reason ->
            exit({Reason, {?MODULE, send, [Process, Msg]}})
    end.

%% Internal

info(Process, Key) ->
    case sbroker_gen:whereis(Process) of
        Pid when is_pid(Pid), node(Pid) == node() ->
            {ask(Pid, Key), Pid};
        undefined ->
            undefined;
        Pid when is_pid(Pid) ->
            {badnode, node(Pid)};
        {_, Node} ->
            {badnode, Node}
    end.

ask(Pid, Key) ->
    try sprotector_server:ask(Pid, Key) of
        Result ->
            Result
    catch
        error:badarg ->
            noprotector
    end.
