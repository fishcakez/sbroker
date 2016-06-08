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
%% @private
-module(sbroker_gen).

-export([call/4]).
-export([simple_call/4]).
-export([async_call/3]).
-export([async_call/4]).
-export([dynamic_call/4]).
-export([whereis/1]).
-export([send/2]).
-export([start_link/4]).
-export([start_link/5]).
-export([init_it/6]).

-compile({no_auto_import, [whereis/1]}).

-define(READ_TIME_AFTER, 16).

-type process() :: pid() | atom() | {atom(), node()} | {global, any()} |
    {via, module(), any()}.
-type name() :: {local, atom()} | {global, any()} | {via, module(), any()}.
-type debug_option() ::
    trace | log | {log, pos_integer()} | statistics |
    {log_to_file, file:filename()} | {install, {fun(), any()}}.
-type start_option() ::
    {debug, debug_option()} | {timeout, timeout()} |
    {spawn_opt, [proc_lib:spawn_option()]} |
    {read_time_after, non_neg_integer() | infinity}.
-type start_return() :: {ok, pid()} | ignore | {error, any()}.

-spec call(Process, Label, Msg, Timeout) -> Reply when
      Process :: process(),
      Label :: atom(),
      Msg :: any(),
      Timeout :: timeout(),
      Reply :: any().
call(Process, Label, Msg, Timeout) ->
    try whereis(Process) of
        undefined ->
            exit({noproc, {?MODULE, call, [Process, Label, Msg, Timeout]}});
        NProcess ->
            try gen:call(NProcess, Label, Msg, Timeout) of
                {ok, Reply} ->
                    Reply
            catch
                exit:Reason ->
                    Args = [Process, Label, Msg, Timeout],
                    exit({Reason, {?MODULE, call, Args}})
            end
    catch
        exit:drop ->
            {drop, 0}
    end.

-spec simple_call(Process, Label, Msg, Timeout) -> Reply when
      Process :: process(),
      Label :: atom(),
      Msg :: any(),
      Timeout :: timeout(),
      Reply :: any().
simple_call(Process, Label, Msg, Timeout) ->
    try gen:call(Process, Label, Msg, Timeout) of
        {ok, Reply} ->
            Reply
    catch
        exit:Reason ->
            Args = [Process, Label, Msg, Timeout],
            exit({Reason, {?MODULE, simple_call, Args}})
    end.

-spec async_call(Process, Label, Msg) -> {await, Tag, NProcess} | {drop, 0} when
      Process :: process(),
      Label :: atom(),
      Msg :: any(),
      Tag :: reference(),
      NProcess :: pid() | {atom(), node()}.
async_call(Process, Label, Msg) ->
    try whereis(Process) of
         undefined ->
            exit({noproc, {?MODULE, async_call, [Process, Label, Msg]}});
        NProcess ->
            Tag = monitor(process, NProcess),
            _ = erlang:send(NProcess, {Label, {self(), Tag}, Msg}, [noconnect]),
            {await, Tag, NProcess}
    catch
        exit:drop ->
            {drop, 0}
    end.

-spec async_call(Process, Label, Msg, To) ->
    {await, Tag, NProcess} | {drop, 0} when
      Process :: process(),
      Label :: atom(),
      Msg :: any(),
      To :: {Pid, Tag},
      Pid :: pid(),
      Tag :: reference(),
      NProcess :: pid() | {atom(), node()}.
async_call(Process, Label, Msg, {Pid, Tag} = To) when is_pid(Pid) ->
    try whereis(Process) of
         undefined ->
            exit({noproc, {?MODULE, async_call, [Process, Label, Msg, Tag]}});
        NProcess ->
            _ = NProcess ! {Label, To, Msg},
            {await, Tag, NProcess}
    catch
        exit:drop ->
            {drop, 0}
    end.

-spec dynamic_call(Process, Label, Msg, Timeout) -> Reply when
      Process :: process(),
      Label :: atom(),
      Msg :: any(),
      Timeout :: timeout(),
      Reply :: any().
dynamic_call(Process, Label, Msg, Timeout) ->
    try whereis(Process) of
        undefined ->
            exit({noproc,
                  {?MODULE, dynamic_call, [Process, Label, Msg, Timeout]}});
        NProcess ->
            Tag = monitor(process, NProcess),
            _ = erlang:send(NProcess, {Label, {self(), Tag}, Msg}, [noconnect]),
            receive
                {Tag, {await, Tag, _} = Await} ->
                    Await;
                {Tag, Reply} ->
                    demonitor(Tag, [flush]),
                    Reply;
                {'DOWN', Tag, _, _, Reason} ->
                    Args = [Process, Label, Msg, Timeout],
                    exit({Reason, {?MODULE, dynamic_call, Args}})
            after
                Timeout ->
                    demonitor(Tag, [flush]),
                    Args = [Process, Label, Msg, Timeout],
                    exit({timeout, {?MODULE, dynamic_call, Args}})
            end
    catch
        exit:drop ->
            {drop, 0}
    end.

-spec send(Process, Msg) -> ok when
      Process :: process(),
      Msg :: any().
send(Process, Msg) ->
    try whereis(Process) of
        undefined ->
            exit({noproc, {?MODULE, send, [Process, Msg]}});
        NProcess ->
            _ = NProcess ! Msg,
            ok
    catch
        exit:drop ->
            exit({drop, {?MODULE, send, [Process, Msg]}})
    end.

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

-spec start_link(Behaviour, Mod, Args, Opts) -> start_return() when
      Behaviour :: module(),
      Mod :: module(),
      Args :: any(),
      Opts :: [start_option()].
start_link(Behaviour, Mod, Args, Opts) ->
    {TimeOpts, GenOpts} = partition_options(Opts),
    gen:start(?MODULE, link, Behaviour, {Mod, Args, TimeOpts}, GenOpts).

-spec start_link(Name, Behaviour, Mod, Args, Opts) -> start_return() when
      Name :: name(),
      Behaviour :: module(),
      Mod :: module(),
      Args :: any(),
      Opts :: [start_option()].
start_link(Name, Behaviour, Mod, Args, Opts) ->
    {TimeOpts, GenOpts} = partition_options(Opts),
    gen:start(?MODULE, link, Name, Behaviour, {Mod, Args, TimeOpts}, GenOpts).

%% gen api

init_it(Starter, Parent, Name, Behaviour, {Mod, Args, TimeOpts}, GenOpts) ->
     _ = put('$initial_call', {Mod, init, 1}),
     Behaviour:init_it(Starter, Parent, Name, Mod, Args, TimeOpts ++ GenOpts).

%% Internal

partition_options(Opts) ->
    ReadAfter = proplists:get_value(read_time_after, Opts, ?READ_TIME_AFTER),
    {[{read_time_after, ReadAfter}], proplists:delete(read_time_after, Opts)}.
