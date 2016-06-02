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
%% @doc This modules provides utility functions for load balancing using the
%% best of two random choices. It is designed for use with `sbroker' and
%% `sregulator' processes using the `sbetter_meter' meter. However any OTP
%% process can use this module to do load balancing using the `via' naming
%% format if the process is registered with and updates the `sbetter_server'.
%%
%% To use `sbetter' with `via' use names of the form
%% `{via, sbetter, {{Broker, ...}, ask | ask_r}}'. Where `{Broker, ...}' is
%% a tuple containing
%% `pid() | atom() | {global, any()} | {via, module(), any()} | {atom(), node()}'.
%% The process with the small value/shortest sojourn time of two random
%% processes for the `ask' (or `ask_r') queue will be called. The sojourn time
%% includes the message queue delay and the time spent waiting in the internal
%% queue.
%%
%% Comparing values/sojourn times requires `ets' lookups. However it is not
%% required to carry out the lookups for every request to get well balanced
%% queues. To only compare two random choices 20% of the time and use
%% `sscheduler' the remaining 80% use `scheduler_ask' and `scheduler_ask_r', or
%% to only compare two random choices 20% of the time and choose a random
%% process the reamining 80% use `rand_ask' and `rand_ask_r'. This ratio is
%% chosen as the majority of the gain in choosing two random choices can be
%% captured by giving 20% of requests a choice. See section 4.5 of the reference
%% for more information.
%%
%% It is not possible to locally look up the pid of a process with name
%% `{atom(), node()}' if the node is not the local node. Therefore a registered
%% name on another node is not supported for use with this module.
%%
%% If a chosen process is not local the call may exit with `{badnode, node()}'.
%%
%% If a chosen process is not registered with the `sbetter_server' the call
%% may exit with `{nobetter, pid()}'. The `sbetter_meter' will register with the
%% server. However other methods can be used to register and update the
%% `sbetter_server'. Registering with the `sbetter_server' must be done with
%% `sbetter_server:register/3' and not using
%% `start_link({via, sbetter, ...}, ...)'.
%%
%% @reference Michael Miztenmacher, The Power of Two Choices in Randomized
%% Load Balancing, 1996.
%% @see sbetter_meter
%% @see sbetter_server
-module(sbetter).

%% public API

-export([whereis_name/1]).
-export([send/2]).

%% types

-type method() ::
    ask | ask_r | scheduler_ask | scheduler_ask_r | rand_ask | rand_ask_r.

-export_type([method/0]).

%% @doc Lookup a pid from a tuple of pids using the best of two random choices
%% for the queue (or possibly using the current scheduler id). If no process is
%% associated with the chosen process returns `undefined'.
-spec whereis_name({Processes, Method}) ->
    Process | undefined when
      Processes :: tuple(),
      Method :: method(),
      Process :: pid().
whereis_name({{}, _}) ->
    undefined;
whereis_name({{Name}, _}) ->
    case sbroker_gen:whereis(Name) of
        Pid when is_pid(Pid) ->
            Pid;
        undefined ->
            undefined;
        {_, Node} ->
            exit({badnode, Node})
    end;
whereis_name({Processes, scheduler_ask}) when is_tuple(Processes) ->
    case scheduler_whereis(Processes, ask) of
        Pid when is_pid(Pid) ->
            Pid;
        undefined ->
            undefined;
        Error ->
            exit(Error)
    end;
whereis_name({Processes, scheduler_ask_r}) when is_tuple(Processes) ->
    case scheduler_whereis(Processes, ask_r) of
        Pid when is_pid(Pid) ->
            Pid;
        undefined ->
            undefined;
        Error ->
            exit(Error)
    end;
whereis_name({Processes, rand_ask}) when is_tuple(Processes) ->
    case rand_whereis(Processes, ask) of
        Pid when is_pid(Pid) ->
            Pid;
        undefined ->
            undefined;
        Error ->
            exit(Error)
    end;
whereis_name({Processes, rand_ask_r}) when is_tuple(Processes) ->
    case rand_whereis(Processes, ask_r) of
        Pid when is_pid(Pid) ->
            Pid;
        undefined ->
            undefined;
        Error ->
            exit(Error)
    end;
whereis_name({Processes, Key}) when is_tuple(Processes) ->
    case better_whereis(Processes, Key) of
        Pid when is_pid(Pid) ->
            Pid;
        undefined ->
            undefined;
        Error ->
            exit(Error)
    end.

%% @doc Sends a message to a process from a tuple of processes using the best of
%% two random choices for the queue (or possibly using the current scheduler
%% id). Returns `ok' if a process could be chosen otherwise exits.
-spec send({Processes, Method}, Msg) ->
    ok when
      Processes :: tuple(),
      Method :: method(),
      Msg :: any().
send(Name, Msg) ->
    case whereis_name(Name) of
        Pid when is_pid(Pid) ->
            _ = Pid ! Msg,
            ok;
        undefined ->
            exit({noproc, {?MODULE, send, [Name, Msg]}})
    end.

%% Internal

scheduler_whereis(Processes, Key) ->
    Size = tuple_size(Processes),
    case scheduler_pick(Size) of
        scheduler ->
            sscheduler:whereis_name(Processes);
        {A, B} ->
            ProcA = element(A, Processes),
            ProcB = element(B, Processes),
            compare(info(ProcA, Key), info(ProcB, Key))
    end.

rand_whereis(Processes, Key) ->
    Size = tuple_size(Processes),
    case rand_pick(Size) of
        {A, B} ->
            ProcA = element(A, Processes),
            ProcB = element(B, Processes),
            compare(info(ProcA, Key), info(ProcB, Key));
        N ->
            Proc = element(N, Processes),
            rand_whereis(Proc)
    end.

rand_whereis(Name) ->
    case sbroker_gen:whereis(Name) of
        {_, Node} ->
            {badnode, Node};
        Other ->
            Other
    end.

better_whereis(Processes, Key) ->
    Size = tuple_size(Processes),
    {A, B} = pick(Size),
    ProcA = element(A, Processes),
    ProcB = element(B, Processes),
    compare(info(ProcA, Key), info(ProcB, Key)).

scheduler_pick(Size) ->
    Pairs = Size * (Size-1),
    case erlang:phash2({self(), make_ref()}, 5 * Pairs) of
        Hash when Hash < Pairs ->
            pick(Hash, Size);
        _ ->
            scheduler
    end.

rand_pick(Size) ->
    Pairs = Size * (Size-1),
    case erlang:phash2({self(), make_ref()}, 5 * Pairs) of
        Hash when Hash < Pairs ->
            pick(Hash, Size);
        Hash ->
            (Hash rem Size) + 1
    end.

pick(Hash, Size) ->
    case {(Hash div Size) + 1, (Hash div (Size-1)) + 1} of
        {Same, Same} ->
            % Same must be less than Size for a match, and first element is
            % 1..Size-1 so adding 1 to creates even distribution.
            {Same+1, Same};
        Other ->
            Other
    end.

pick(Size) ->
    Pairs = Size * (Size-1),
    Hash = erlang:phash2({self(), make_ref()}, Pairs),
    pick(Hash, Size).

info(Process, Key) ->
    case sbroker_gen:whereis(Process) of
        Pid when is_pid(Pid), node(Pid) == node() ->
            {lookup(Pid, Key), Pid};
        undefined ->
            undefined;
        Pid when is_pid(Pid) ->
            {badnode, node(Pid)};
        {_, Node} ->
            {badnode, Node}
    end.

lookup(Pid, Key) ->
    try sbetter_server:lookup(Pid, Key) of
        Value ->
            Value
    catch
        error:badarg ->
            nobetter
    end.

compare({ValueA, Pid}, {ValueOrError, _})
  when is_integer(ValueA), ValueA < ValueOrError ->
    Pid;
compare(_, {ValueB, Pid}) when is_integer(ValueB) ->
    Pid;
compare({ValueA, Pid}, _) when is_integer(ValueA) ->
    Pid;
compare(undefined, _) ->
    undefined;
compare(_, undefined) ->
    undefined;
compare(Error, _) ->
    Error.
