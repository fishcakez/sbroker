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
%% @doc
%% This module provides a job regulator for controlling the level of concurrency
%% of processes carrying out a task. A process requests permission to run and is
%% queued until it is allowed to begin. Once the task is complete the process
%% informs the regulator that is is done. Alternatively the process can ask if
%% it can continue running and gains priority over any queued processes. The
%% queue is managed using an `sbroker_queue' callback module, and the level
%% of concurrency by an `sregulator_valve' callback module. The message queue
%% delay and processing delay are monitorred by an `sbroker_meter'.
%%
%% The main function to ask to begin is `ask/1', which blocks until the request
%% is accepted or the queue drops the request. `done/3' is then called after the
%% task has finished or `continue/2' to continue.
%%
%% A regulator requires a callback module to be configured, in a similar way to
%% a supervisor's children are specified. The callback modules implements one
%% callback, `init/1', with single argument `Args'. `init/1' should return
%% `{ok, {QueueSpec, ValveSpec, MeterSpec}}' or `ignore'. `QueueSpec' is the
%% `sbroker_queue' specification, `ValveSpec' is the `sregulator_valve'
%% specification and `MeterSpec' is the `sbroker_meter' specification. All three
%% take the same format: `{Module, Args}', where `Module' is the callback module
%% and `Args' the arguments term for the module. In the case of `ignore' the
%% regulator is not started and `start_link' returns `ignore'. As the callback
%% modules are defined in the `init/1' callback a regulator supports the
%% `dynamic' modules supervisor child specification.
%%
%%
%% For example:
%%
%% ```
%% -module(sregulator_example).
%%
%% -behaviour(sregulator).
%%
%% -export([start_link/0]).
%% -export([ask/0]).
%% -export([continue/1]).
%% -export([done/1]).
%% -export([init/1]).
%%
%% start_link() ->
%%     sregulator:start_link({local, ?MODULE}, ?MODULE, [], []).
%%
%% ask() ->
%%     case sregulator:ask(?MODULE) of
%%         {go, Ref, _, _, _} -> {ok, Ref};
%%         {drop, _}          -> {error, dropped}
%%     end.
%%
%% continue(Ref) ->
%%     case sregulator:continue(?MODULE, Ref, 5000) of
%%        {go, Ref, _, _, _} -> {ok, Ref};
%%        {done, _}          -> {error, dropped};
%%        {not_found, _}     -> {error, not_found}
%%     end.
%%
%% done(Ref
%%
%% init([]) ->
%%   QueueSpec = {sbroker_codel_queue, {out, 50, 500, drop_r, 64}},
%%   ValveSpec = {sregulator_open_valve, 30},
%%   MeterSpec = {sbroker_alarm_meter, {50, 500, {?MODULE, overload}}},
%%   {ok, {QueueSpec, ValveSpec, MeterSpec}}.
%% '''
-module(sregulator).

%% public api

-export([ask/1]).
-export([nb_ask/1]).
-export([async_ask/1]).
-export([async_ask/2]).
-export([await/2]).
-export([cancel/3]).
-export([continue/2]).
-export([done/3]).
-export([update/3]).
-export([cast/2]).
-export([change_config/2]).
-export([len/2]).
-export([size/2]).
-export([start_link/3]).
-export([start_link/4]).

%% test api

-export([timeout/1]).

%% gen api

-export([init_it/6]).

%% sys api

-export([system_continue/3]).
-export([system_code_change/4]).
-export([system_get_state/1]).
-export([system_replace_state/2]).
-export([system_terminate/4]).
-export([format_status/2]).

%% types

-type regulator() :: pid() | atom() | {atom(), node()} | {global, any()}
    | {via, module(), any()}.
-type name() :: {local, atom()} | {global, any()} | {via, module(), any()}.
-type debug_option() ::
    trace | log | {log, pos_integer()} | statistics |
    {log_to_file, file:filename()} | {install, {fun(), any()}}.
-type start_option() ::
    {debug, debug_option()} | {timeout, timeout()} |
    {spawn_opt, [proc_lib:spawn_option()]} | {time_module, module()} |
    {read_time_after, non_neg_integer() | infinity}.
-type start_return() :: {ok, pid()} | ignore | {error, any()}.
-type handler_spec() :: {module(), any()}.

-export_type([regulator/0]).
-export_type([handler_spec/0]).

-callback init(Args :: any()) ->
    {ok, {QueueSpec :: handler_spec(), ValveSpec :: handler_spec(),
          MeterSpec :: handler_spec()}} | ignore.

-record(config, {mod :: module(),
                 args :: any(),
                 parent :: pid(),
                 dbg :: [sys:dbg_opt()],
                 name :: name() | pid(),
                 time_mod :: module(),
                 queue_mod :: module(),
                 valve_mod :: module(),
                 meter_mod :: module()}).

-record(time, {now :: integer(),
               send :: integer(),
               open :: integer(),
               next = infinity :: integer() | infinity,
               seq :: non_neg_integer(),
               read_after :: non_neg_integer() | infinity,
               meter :: any()}).

-dialyzer(no_return).

%% public api

%% @doc Send a run request to the regulator, `Regulator'.
%%
%% Returns `{go, Ref, RegulatorPid, RelativeTIme, SojournTime}' on successfully
%% being allowed to run or `{drop, SojournTime}'.
%%
%% `Ref' is the lock reference, which is a `reference()'. `RegulatorPid' is the
%% `pid()' of the regulator process. `RelativeTime' is the time difference
%% between when the request was sent and the message that opened the regulator's
%% valve was sent. `SojournTime' is the approximate time spent in both the
%% regulator's message queue and internal queue.
%%
%% `RelativeTime' represents the `SojournTime' without the overhead of the
%% regulator. The value measures the queue congestion without being effected by
%% the load of the regulator or node.
%%
%% If `RelativeTime' is positive, the request was enqueued in the internal queue
%% awaiting a message to open the value that was sent approximately
%% `RelativeTime' ater this request was sent. Therefore `SojournTime' minus
%% `RelativeTime' is the latency, or overhead, of the regulator.
%%
%% If `RelativeTime' is negative, the regulator's valve was opened by a message
%% sent `abs(RelativeTime)' before this request. Therefore `SojournTime' is the
%% latency, or overhead, of the regulator.
%%
%% If `RelativeTime' is `0', the request was sent at approximately the same as
%% the message that open the regulator's valve.
-spec ask(Regulator) -> Go | Drop when
      Regulator :: regulator(),
      Go :: {go, Ref, Value, RelativeTime, SojournTime},
      Ref :: reference(),
      Value :: any(),
      RelativeTime :: integer(),
      SojournTime :: non_neg_integer(),
      Drop :: {drop, SojournTime}.
ask(Regulator) ->
    sbroker_gen:call(Regulator, ask, self(), infinity).

%% @doc Send a run request to the regulator, `Regulator`, but do not enqueue the
%% request if not immediately allowed to run.
%%
%% Returns `{go, Ref, RegulatorPid, RelativeTime, SojournTime}' on successfully
%% being allowed to run or `{retry, SojournTime}'.
%%
%% `Ref' is the lock reference, which is a `reference()'. `RegulatorPid' is the
%% `pid()' of the regulator process. `RelativeTime' is the time difference
%% between when the request was sent and the message that opened the regulator's
%% valve was sent. `SojournTime' is the approximate time spent in the
%% regulator's message queue.
%%
%% @see ask/1
-spec nb_ask(Regulator) -> Go | Retry when
      Regulator :: regulator(),
      Go :: {go, Ref, Value, RelativeTime, SojournTime},
      Ref :: reference(),
      Value :: any(),
      RelativeTime :: 0 | neg_integer(),
      SojournTime :: non_neg_integer(),
      Retry :: {retry, SojournTime}.
nb_ask(Regulator) ->
    sbroker_gen:call(Regulator, nb_ask, self(), infinity).

%% @doc Monitor the regulator and send an asynchronous run request. Returns
%% `{await, Tag, Process}'.
%%
%% `Tag' is a monitor `reference()' that uniquely identifies the reply
%% containing the result of the request. `Process' is the `pid()' of the
%% regulator or `{atom(), node()}' if the regulator is registered locally on a
%% different node.
%%
%% The reply is of the form `{Tag, Msg}' where `Msg' is either
%% `{go, Ref, RegulatorPid, RelativeTime, SojournTime}' or
%% `{drop, SojournTime}'.
%%
%% `Ref' is the lock reference, which is a `reference()'. `RegulatorPid' is the
%% `pid()' of the regulator process. `RelativeTime' is the time difference
%% between when the request was sent and the message that opened the regulator's
%% valve was sent. `SojournTime' is the approximate time spent in both the
%% regulator's message queue and internal queue.
%%
%% Multiple asynchronous requests can be made from a single process to a
%% regulator and no guarantee is made of the order of replies. A process making
%% multiple requests can reuse the monitor reference for subsequent requests to
%% the same regulator process (`Process') using `async_ask/2'.
%%
%% @see cancel/2
%% @see async_ask/2
-spec async_ask(Regulator) -> {await, Tag, Pid} when
      Regulator :: regulator(),
      Tag :: reference(),
      Pid :: pid().
async_ask(Regulator) ->
    sbroker_gen:async_call(Regulator, ask, self()).

%% @doc Send an asynchronous run request using tag, `Tag'. Returns
%% `{await, Tag, Process}'.
%%
%% `Tag' is any term, `any()', that idenitifes the reply containing the result
%% of the request. `Process' is the `pid()' of the regulator or
%% `{atom(), node()}' if the regulator is registered locally on a different
%% node.
%%
%% Otherwise this function is equivalent to `async_ask/1'.
%%
%% @see async_ask/1
%% @see cancel/2
-spec async_ask(Regulator, Tag) -> {await, Tag, Process} when
      Regulator :: regulator(),
      Tag :: any(),
      Process :: pid() | {atom(), node()}.
async_ask(Regulator, Tag) ->
    sbroker_gen:async_call(Regulator, ask, self(), Tag).

%% @doc Await the response to an asynchronous request idenitifed by `Tag'.
%%
%% Exits if a response is not received after `Timeout' milliseconds.
%%
%% Exits if a `DOWN' message is received with reference `Tag'.
%%
%% @see async_ask/1
%% @see async_ask/2
-spec await(Tag, Timeout) -> Go | Drop when
      Tag :: any(),
      Timeout :: timeout(),
      Go :: {go, Ref, Value, RelativeTime, SojournTime},
      Ref :: reference(),
      Value :: any(),
      RelativeTime :: integer(),
      SojournTime :: non_neg_integer(),
      Drop :: {drop, SojournTime}.
await(Tag, Timeout) ->
    receive
        {Tag, {go, _, _, _, _} = Reply} ->
            Reply;
        {Tag, {drop, _} = Reply} ->
            Reply;
        {'DOWN', Tag, _, _, Reason} when is_reference(Tag) ->
            exit({Reason, {?MODULE, await, [Tag, Timeout]}})
    after
        Timeout ->
            exit({timeout, {?MODULE, await, [Tag, Timeout]}})
    end.

%% @doc Cancel an asynchronous request.
%%
%% Returns the number of cancelled requests or `false' if no requests exist with
%% tag `Tag'. In the later case a caller may wish to check is message queue for
%% an existing reply.
%%
%% @see async_ask/1
%% @see async_ask/2
-spec cancel(Regulator, Tag, Timeout) -> Count | false when
      Regulator :: regulator(),
      Tag :: any(),
      Timeout :: timeout(),
      Count :: pos_integer().
cancel(Regulator, Tag, Timeout) ->
    sbroker_gen:call(Regulator, cancel, Tag, Timeout).

%% @doc Send a request to continue running using an existing lock reference,
%% `Ref'. The request is not queued.
%%
%% Returns `{go, Ref, RegulatorPid, RelativeTime, SojournTime}' on successfully
%% being allowed to run, `{stop, SojournTime}' when the process should stop
%% running or `{not_found, SojournTime}' when the lock reference does not exist
%% on the regulator.
%%
%% `Ref' is the lock reference, which is a `reference()'. `RegulatorPid' is the
%% `pid()' of the regulator process. `RelativeTime' is the time difference
%% between when the request was sent and the message that opened the regulator's
%% valve was sent. `SojournTime' is the approximate time spent in the
%% regulator's message queue.
%%
%% @see ask/1
-spec continue(Regulator, Ref) -> Go | Stop | NotFound when
      Regulator :: regulator(),
      Ref :: reference(),
      Go :: {go, Ref, Pid, RelativeTime, SojournTime},
      Ref :: reference(),
      Pid :: pid(),
      RelativeTime :: integer(),
      SojournTime :: non_neg_integer(),
      Stop :: {stop, SojournTime},
      NotFound :: {not_found, SojournTime}.
continue(Regulator, Ref) ->
    sbroker_gen:call(Regulator, continue, Ref, infinity).

%% @doc Inform the regulator the process has finished running and release the
%% lock, `Ref'.
%%
%% Returns `{stop, SojournTime}' if the regulator acknowledged the process has
%% stopped running or `{not_found, SojournTime}' if the lock reference, `Ref',
%% does not exist on the regulator.
%%
%% `SojournTime' is the time the request spent in the regulator's message queue.
%%
%% @see ask/1
-spec done(Regulator, Ref, Timeout) -> Stop | NotFound when
      Regulator :: regulator(),
      Ref :: reference(),
      Timeout :: timeout(),
      Stop :: {stop, SojournTime},
      SojournTime :: non_neg_integer(),
      NotFound :: {not_found, SojournTime}.
done(Regulator, Ref, Timeout) ->
    sbroker_gen:call(Regulator, done, Ref, Timeout).

%% @doc Synchronously update the valve in the regulator.
%%
%% `Value' is an `integer()' and `Timeout' is the timout, `timeout()', to wait
%% in milliseconds for the regulator to reply to the update.
%%
%% Returns `ok'.
-spec update(Regulator, Value, Timeout) -> ok when
      Regulator :: regulator(),
      Value :: integer(),
      Timeout :: timeout().
update(Regulator, Value, Timeout) when is_integer(Value) ->
    sbroker_gen:call(Regulator, update, Value, Timeout).

%% @doc Update the valve in the regulator without waiting for the regulator to
%% handle the update.
%%
%% `Value' is an `integer()'.
%%
%% Returns `ok'.
-spec cast(Regulator, Value) -> ok when
      Regulator :: regulator(),
      Value :: integer().
cast(Regulator, Value) when is_integer(Value) ->
    sbroker_gen:send(Regulator, {update, cast, Value}).

%% @doc Change the configuration of the regulator. Returns `ok' on success and
%% `{error, Reason}' on failure, where `Reason' is the reason for failure.
%%
%% The regulators calls the `init/1' callback to get the new configuration. If
%% `init/1' returns `ignore' the config does not change.
-spec change_config(Regulator, Timeout) -> ok | {error, Reason} when
      Regulator :: regulator(),
      Timeout :: timeout(),
      Reason :: any().
change_config(Regulator, Timeout) ->
    sbroker_gen:call(Regulator, change_config, undefined, Timeout).

%% @doc Get the length of the internal queue in the regulator, `Regulator'.
-spec len(Regulator, Timeout) -> Length when
      Regulator :: regulator(),
      Timeout :: timeout(),
      Length :: non_neg_integer().
len(Regulator, Timeout) ->
    sbroker_gen:call(Regulator, len, undefined, Timeout).

%% @doc Get the number of processes holding a lock with the regulator,
%% `Regulator'.
-spec size(Regulator, Timeout) -> Size when
      Regulator :: regulator(),
      Timeout :: timeout(),
      Size :: non_neg_integer().
size(Regulator, Timeout) ->
    sbroker_gen:call(Regulator, size, undefined, Timeout).

%% @doc Starts a regulator with callback module `Module' and argument `Args',
%% and regulator options `Opts'.
%%
%% `Opts' is a `proplist' and supports `debug', `timeout' and `spawn_opt' used
%% by `gen_server' and `gen_fsm'. `time_module' sets the `sbroker_time'
%% callback module, which defaults to `erlang' if `erlang:monotonic_time/0' is
%% exported, otherwise `sbroker_legacy'. `read_time_after' sets the number of
%% requests when a cached time is stale and the time is read again. Its value is
%% `non_neg_integer()' or `infinity' and defaults to `16'.
%%
%% @see gen_server:start_link/3
%% @see sbroker_time
-spec start_link(Module, Args, Opts) -> StartReturn when
      Module :: module(),
      Args :: any(),
      Opts :: [start_option()],
      StartReturn :: start_return().
start_link(Mod, Args, Opts) ->
    sbroker_gen:start_link(?MODULE, Mod, Args, Opts).

%% @doc Starts a regulator with name `Name', callback module `Module' and
%% argument `Args', and regulator options `Opts'.
%%
%% @see start_link/3
-spec start_link(Name, Module, Args, Opts) -> StartReturn when
      Name :: name(),
      Module :: module(),
      Args :: any(),
      Opts :: [start_option()],
      StartReturn :: start_return().
start_link(Name, Mod, Args, Opts) ->
    sbroker_gen:start_link(Name, ?MODULE, Mod, Args, Opts).

%% test api

%% @hidden
-spec timeout(Regulator) -> ok when
      Regulator :: regulator().
timeout(Regulator) ->
    sbroker_gen:send(Regulator, timeout).

%% gen api

%% @private
init_it(Starter, Parent, Name, Mod, Args, Opts) ->
    DbgOpts = proplists:get_value(debug, Opts, []),
    Dbg = sys:debug_options(DbgOpts),
    {TimeMod, ReadAfter} = time_options(Opts),
    try Mod:init(Args) of
        {ok, {{QMod, QArgs}, {VMod, VArgs}, {MMod, MArgs}}} ->
            Config = #config{mod=Mod, args=Args, parent=Parent, dbg=Dbg,
                             name=Name, time_mod=TimeMod, queue_mod=QMod,
                             valve_mod=VMod, meter_mod=MMod},
            Now = monotonic_time(Config),
            Time = #time{now=Now, send=Now, open=Now, read_after=ReadAfter},
            init(Starter, Time, QArgs, VArgs, MArgs, Config);
        ignore ->
            init_stop(Starter, Name, ignore, normal);
        Other ->
            Reason = {bad_return_value, Other},
            init_stop(Starter, Name, Reason)
    catch
        Class:Reason ->
            Stack = erlang:get_stacktrace(),
            Reason2 = sbroker_handlers:exit_reason({Class, Reason, Stack}),
            init_stop(Starter, Name, Reason2)
    end.

%% sys API

%% @private
system_continue(Parent, Dbg, [State, Time, Q, V, Config]) ->
    NConfig = Config#config{parent=Parent, dbg=Dbg},
    timeout(State, Time, Q, V, NConfig);
system_continue(Parent, Dbg,
                {change, Change, [State, Time, Q, V, Config]}) ->
    NConfig = Config#config{parent=Parent, dbg=Dbg},
    change(State, Change, Time, Q, V, NConfig).

%% @private
system_code_change([_, _, _, _, #config{mod=Mod} = Config] = Misc, Mod, _, _) ->
    case config_change(Config) of
        {ok, Change} ->
            {ok, {change, Change, Misc}};
        ignore ->
            {ok, Misc};
        {error, Reason} ->
            % sys will turn this into {error, Reason}
            Reason
    end;
system_code_change([_, _, _, _, _] = Misc, Mod, OldVsn, Extra) ->
    {ok, code_change(Misc, Mod, OldVsn, Extra)};
system_code_change({change, Change,
                    [_, _, _, _, #config{mod=Mod} = Config] = Misc}, Mod,
                   _, _) ->
    case config_change(Config) of
        {ok, NChange} ->
            {ok, {change, NChange, Misc}};
        ignore ->
            {ok, {change, Change, Misc}};
        {error, Reason} ->
            % sys will turn this into {error, Reason}
            Reason
    end;
system_code_change({change, Change, Misc}, Mod, OldVsn, Extra) ->
    {ok, {change, Change, code_change(Misc, Mod, OldVsn, Extra)}}.

%% @private
system_get_state([_, #time{meter=M}, Q, V,
                  #config{queue_mod=QMod, valve_mod=VMod, meter_mod=MMod}]) ->
    Callbacks = [{QMod, queue, Q},
                 {VMod, valve, V},
                 {MMod, meter, M}],
    {ok, Callbacks};
system_get_state({change, _, Misc}) ->
    system_get_state(Misc).

%% @private
system_replace_state(Replace,
                     [State, #time{meter=M} = Time, Q, V,
                      #config{queue_mod=QMod, valve_mod=VMod,
                              meter_mod=MMod} = Config]) ->
    {QMod, queue, NQ} = QRes = Replace({QMod, queue, Q}),
    {VMod, valve, NV} = VRes = Replace({VMod, valve, V}),
    {MMod, meter, NM} = MRes = Replace({MMod, meter, M}),
    Result = [QRes, VRes, MRes],
    Misc = [State, Time#time{meter=NM}, NQ, NV, Config],
    {ok, Result, Misc};
system_replace_state(Replace, {change, Change, Misc}) ->
    {ok, States, NMisc} = system_replace_state(Replace, Misc),
    {ok, States, {change, Change, NMisc}}.

%% @private
system_terminate(Reason, Parent, Dbg, [State, Time, Q, V, Config]) ->
    NConfig = Config#config{parent=Parent, dbg=Dbg},
    terminate({stop, Reason}, State, Time, Q, V, NConfig).

%% @private
format_status(Opt,
              [PDict, SysState, Parent, _,
               [State, #time{now=Now, meter=M}, Q, V,
                #config{name=Name, queue_mod=QMod, valve_mod=VMod,
                        meter_mod=MMod, time_mod=TimeMod}]]) ->
    Header = gen:format_status_header("Status for sregulator", Name),
    Handlers = [{QMod, queue, Q}, {VMod, valve, V}, {MMod, meter, M}],
    Handlers2 = [{Mod, Id, format_status(Mod, Opt, PDict, Handler)} ||
                {Mod, Id, Handler} <- Handlers],
    [{header, Header},
     {data, [{"Status", SysState},
             {"Parent", Parent},
             {"State", State},
             {"Time", {TimeMod, Now}}]},
     {items, {"Installed handlers", Handlers2}}];
format_status(Opt, [PDict, SysState, Parent, Dbg, {change, _, Misc}]) ->
    format_status(Opt, [PDict, SysState, Parent, Dbg, Misc]).

%% Internal

time_options(Opts) ->
    TimeMod = proplists:get_value(time_module, Opts),
    ReadAfter = proplists:get_value(read_time_after, Opts),
    {TimeMod, ReadAfter}.

init(Starter, #time{now=Now} = Time, QArgs, VArgs, MArgs,
     #config{queue_mod=QMod, valve_mod=VMod, meter_mod=MMod,
             name=Name} = Config) ->
    Inits = [{sbroker_queue, QMod, QArgs}, {sregulator_valve, VMod, VArgs},
             {sbroker_meter, MMod, MArgs}],
    case sbroker_handlers:init(Now, Inits, report_name(Config)) of
        {ok, [{_, _, Q, QNext}, {_, _, {State, V}, VNext},
              {_, _, M, MNext}]} ->
            Next = min(QNext, VNext),
            NTime = Time#time{meter=M, next=MNext},
            enter_loop(Starter, NTime, State, Q, V, Next, Config);
        {stop, Reason} ->
            init_stop(Starter, Name, Reason)
    end.

init_stop(Starter, Name, Reason) ->
    init_stop(Starter, Name, {error, Reason}, Reason).

init_stop(Starter, Name, Ack, Reason) ->
    unregister_name(Name),
    proc_lib:init_ack(Starter, Ack),
    exit(Reason).

unregister_name({local, Name}) ->
    unregister(Name);
unregister_name({global, Name}) ->
    global:unregister_name(Name);
unregister_name({via, Mod, Name}) ->
    Mod:unregister_name(Name);
unregister_name(Self) when is_pid(Self) ->
    ok.

enter_loop(Starter, Time, State, Q, V, Next, Config) ->
    proc_lib:init_ack(Starter, {ok, self()}),
    Timeout = idle_timeout(Time, Next, Config),
    idle_recv(State, Timeout, Time, Q, V, Config).

monotonic_time(#config{time_mod=TimeMod}) ->
    TimeMod:monotonic_time().

mark(Time, Config) ->
    Now = monotonic_time(Config),
    _ = self() ! {'$mark', Now},
    Time#time{now=Now, send=Now, seq=0}.

update_time(State, #time{seq=Seq, read_after=Seq} = Time, Q, V, Config) ->
    Now = monotonic_time(Config),
    update_meter(Now, State, Time, Q, V, Config);
update_time(_, #time{seq=Seq} = Time, _, _, _) ->
    Time#time{seq=Seq+1}.

update_meter(Now, State,
             #time{now=Prev, send=Send, seq=Seq, meter=Meter} = Time, Q, V,
             #config{meter_mod=MeterMod} = Config) ->
    ProcessDelay = (Now - Prev) div Seq,
    %% Remove one ProcessDelay to estimate time last message was received.
    %% NB: This gives correct QueueDelay of 0 when single message was received.
    QueueDelay = (Now - ProcessDelay) - Send,
    try MeterMod:handle_update(QueueDelay, ProcessDelay, Now, Meter) of
        {NMeter, Next} ->
            Time#time{now=Now, seq=0, meter=NMeter, next=Next};
        Other ->
            meter_return(Other, State, Time, Q, V, Config)
    catch
        Class:Reason ->
            meter_exception(Class, Reason, State, Time, Q, V, Config)
    end.

idle(State, #time{seq=0} = Time, Q, V, Next, Config) ->
    Timeout = idle_timeout(Time, Next, Config),
    idle_recv(State, Timeout, Time, Q, V, Config);
idle(State, Time, Q, V, Next, Config) ->
    Now = monotonic_time(Config),
    NTime = update_meter(Now, State, Time, Q, V, Config),
    Timeout = idle_timeout(NTime, Next, Config),
    idle_recv(State, Timeout, NTime, Q, V, Config).

idle_timeout(#time{now=Now, next=Next1}, Next2, #config{time_mod=TimeMod}) ->
    case min(Next1, Next2) of
        infinity ->
            infinity;
        Next ->
            Diff = Next-Now,
            Timeout = TimeMod:convert_time_unit(Diff, native, milli_seconds),
            max(Timeout, 1)
    end.

idle_recv(State, Timeout, Time, Q, V, Config) ->
    receive
        Msg ->
            NTime = mark(Time, Config),
            handle(State, Msg, NTime, Q, V, infinity, Config)
    after
        Timeout ->
            NTime = mark(Time, Config),
            timeout(State, NTime, Q, V, Config)
    end.


handle(open, Msg, Time, Q, V, Next, Config) ->
    open(Msg, Time, Q, V, Next, Config);
handle(closed, Msg, Time, Q, V, Next, Config) ->
    closed(Msg, Time, Q, V, Next, Config).

open(Time, Q, V, Next, Config) ->
    receive
        Msg ->
            NTime = update_time(open, Time, Q, V, Config),
            open(Msg, NTime, Q, V, Next, Config)
    end.

open({Tag, Ask, Pid}, #time{now=Now, send=Send, open=Open} = Time, Q, V, _,
     #config{valve_mod=VMod} = Config) when Tag == ask; Tag == nb_ask ->
    Ref = monitor(process, Pid),
    go(Ask, Ref, Send, Open, Now),
    try VMod:handle_ask(Pid, Ref, Now, V) of
        {open, NV, Next} ->
            open(Time, Q, NV, Next, Config);
        {closed, NV, Next} ->
            closed(Time, Q, NV, Next, Config);
        Other ->
            valve_return(Other, open, Time, Q, V, Config)
    catch
        Class:Reason ->
            valve_exception(Class, Reason, open, Time, Q, V, Config)
    end;
open({cancel, From, _}, Time, Q, V, _, Config) ->
    gen:reply(From, false),
    timeout(open, Time, Q, V, Config);
open(Msg, Time, Q, V, Next, Config) ->
    common(Msg, open, Time, Q, V, Next, Config).

closed(Time, Q, V, Next, Config) ->
    receive
        Msg ->
            NTime = update_time(closed, Time, Q, V, Config),
            closed(Msg, NTime, Q, V, Next, Config)
    end.

closed({ask, Ask, Pid}, #time{now=Now, send=Send} = Time, Q, V, _,
       #config{queue_mod=QMod} = Config) ->
    try QMod:handle_in(Send, Ask, Pid, Now, Q) of
        {NQ, Next} ->
            valve_timeout(Time, NQ, V, Next, Config);
        Other ->
            queue_return(Other, closed, Time, Q, V, Config)
    catch
        Class:Reason ->
            queue_exception(Class, Reason, closed, Time, Q, V, Config)
    end;
closed({nb_ask, Ask, _}, #time{now=Now, send=Send} = Time, Q, V, _, Config) ->
    retry(Ask, Send, Now),
    closed_timeout(Time, Q, V, Config);
closed({cancel, From, Tag}, #time{now=Now} = Time, Q, V, _,
       #config{queue_mod=QMod} = Config) ->
    try QMod:handle_cancel(Tag, Now, Q) of
        {Reply, NQ, Next} ->
            gen:reply(From, Reply),
            valve_timeout(Time, NQ, V, Next, Config);
        Other ->
            queue_return(Other, closed, Time, Q, V, Config)
    catch
        Class:Reason ->
            queue_exception(Class, Reason, closed, Time, Q, V, Config)
    end;
closed(Msg, Time, Q, V, Next, Config) ->
    common(Msg, closed, Time, Q, V, Next, Config).

common({update, From, Value}, State, #time{now=Now} = Time, Q, V, _,
       #config{valve_mod=VMod} = Config) ->
    try VMod:handle_update(Value, Now, V) of
        {NState, NV, Next}  when NState == open; NState == closed ->
            updated(From),
            timeout(State, NState, Time, Q, NV, Next, Config);
        Other ->
            valve_return(Other, State, Time, Q, V, Config)
    catch
        Class:Reason ->
            valve_exception(Class, Reason, State, Time, Q, V, Config)
    end;
common({continue, From, Ref}, State,
       #time{now=Now, open=Open, send=Send} = Time, Q, V, _,
       #config{valve_mod=VMod} = Config) ->
    try VMod:handle_continue(Ref, Now, V) of
        {continue, NState, NV, Next} when NState == open; NState == closed ->
            go(From, Ref, Send, Open, Now),
            timeout(State, NState, Time, Q, NV, Next, Config);
        {done, NState, NV, Next} when NState == open; NState == closed ->
            stop(From, Send, Now),
            timeout(State, NState, Time, Q, NV, Next, Config);
        {error, NState, NV, Next} when NState == open; NState == closed ->
            not_found(From, Send, Now),
            timeout(State, NState, Time, Q, NV, Next, Config);
        Other ->
            valve_return(Other, State, Time, Q, V, Config)
    catch
        Class:Reason ->
            valve_exception(Class, Reason, State, Time, Q, V, Config)
    end;
common({done, From, Ref}, State, #time{now=Now, send=Send} = Time, Q, V, _,
       #config{valve_mod=VMod} = Config) ->
    try VMod:handle_done(Ref, Now, V) of
        {done, NState, NV, Next} when NState == open; NState == closed ->
            stop(From, Send, Now),
            timeout(State, NState, Time, Q, NV, Next, Config);
        {error, NState, NV, Next} when NState == open; NState == closed ->
            not_found(From, Send, Now),
            timeout(State, NState, Time, Q, NV, Next, Config);
        Other ->
            valve_return(Other, State, Time, Q, V, Config)
    catch
        Class:Reason ->
            valve_exception(Class, Reason, State, Time, Q, V, Config)
    end;
common({'$mark', Mark}, State, #time{now=Now} = Time, Q, V, Next,
       Config) ->
    receive
        Msg ->
            _ = self() ! {'$mark', Now},
            Send = (Mark + Now) div 2,
            handle(State, Msg, Time#time{send=Send}, Q, V, Next, Config)
    after
        0 ->
            idle(State, Time, Q, V, Next, Config)
    end;
common({'EXIT', Parent, Reason}, State, Time, Q, V, _,
       #config{parent=Parent} = Config) ->
    terminate({stop, Reason}, State, Time, Q, V, Config);
common({system, From, Msg}, State, Time, Q, V, _, Config) ->
    system(From, Msg, State, Time, Q, V, Config);
common({change_config, From, _}, State, Time, Q, V, _, Config) ->
    config_change(From, State, Time, Q, V, Config);
common({len, From, _}, State, Time, Q, V, _,
       #config{queue_mod=QMod} = Config) ->
    try QMod:len(Q) of
        Len ->
            gen:reply(From, Len),
            timeout(State, Time, Q, V, Config)
    catch
        Class:Reason ->
            queue_exception(Class, Reason, State, Time, Q, V, Config)
    end;
common({size, From, _}, State, Time, Q, V, _,
       #config{valve_mod=VMod} = Config) ->
    try VMod:size(V) of
        Size ->
            gen:reply(From, Size),
            timeout(State, Time, Q, V, Config)
    catch
        Class:Reason ->
            queue_exception(Class, Reason, State, Time, Q, V, Config)
    end;
common({_, From, get_modules}, State, Time, Q, V, _,
       #config{mod=Mod, queue_mod=QMod, valve_mod=VMod,
               meter_mod=MeterMod} = Config) ->
    gen:reply(From, lists:usort([Mod, QMod, VMod, MeterMod])),
    timeout(State, Time, Q, V, Config);
common(timeout, State, Time, Q, V, _, Config) ->
    timeout(State, Time, Q, V, Config);
common(Msg, State, Time, Q, V, _, Config) ->
    info_queue(Msg, State, Time, Q, V, Config).

go(From, Ref, Send, Open, Now) ->
    RelativeTime = Open - Send,
    SojournTime = Now - Send,
    gen:reply(From, {go, Ref, self(), RelativeTime, SojournTime}).

retry(From, Send, Now) ->
    gen:reply(From, {retry, Now - Send}).

updated(cast) ->
    ok;
updated(From) ->
    gen:reply(From, ok).

stop(From, Send, Now) ->
    gen:reply(From, {stop, Now - Send}).

not_found(From, Send, Now) ->
    gen:reply(From, {not_found, Now - Send}).

system(From, Msg, State, Time, Q, V,
       #config{parent=Parent, dbg=Dbg} = Config) ->
    NConfig = Config#config{dbg=[]},
    sys:handle_system_msg(Msg, From, Parent, ?MODULE, Dbg,
                          [State, Time, Q, V, NConfig]).

format_status(Mod, Opt, PDict, State) ->
    case erlang:function_exported(Mod, format_status, 2) of
        true ->
            try Mod:format_status(Opt, [PDict, State]) of
                Status ->
                    Status
            catch
                _:_ ->
                    State
            end;
        false ->
            State
    end.

code_change([State, #time{now=Now, meter=Meter, next=MNext} = Time, Q, V,
             #config{queue_mod=QMod, valve_mod=VMod,
                     meter_mod=MeterMod} = Config], Mod, OldVsn, Extra) ->
    Callbacks = [{sbroker_queue, QMod, Q, infinity},
                 {sregulator_valve, VMod, {State, V}, infinity},
                 {sbroker_meter, MeterMod, Meter, MNext}],
    NCallbacks = sbroker_handlers:code_change(Now, Callbacks, Mod, OldVsn,
                                              Extra),
    [{sbroker_queue, QMod, NQ, _},
     {sregulator_valve, VMod, {NState, NV}, _},
     {sbroker_meter, MeterMod, NMeter, NMNext}] = NCallbacks,
    [NState, Time#time{meter=NMeter, next=NMNext}, NQ, NV, Config].

config_change(From, State, Time, Q, V, Config) ->
    case config_change(Config) of
        {ok, Change} ->
            gen:reply(From, ok),
            change(State, Change, Time, Q, V, Config);
        ignore  ->
            gen:reply(From, ok),
            timeout(State, Time, Q, V, Config);
        {error, Reason} ->
            gen:reply(From, {error, Reason}),
            timeout(State, Time, Q, V, Config)
    end.

config_change(#config{mod=Mod, args=Args}) ->
    try Mod:init(Args) of
        {ok, {{QMod, QArgs}, {VMod, VArgs}, {MMod, MArgs}}} ->
            {ok, {QMod, QArgs, VMod, VArgs, MMod, MArgs}};
        ignore ->
            ignore;
        Other ->
            {error, {bad_return_value, Other}}
    catch
        Class:Reason ->
            {error, {Class, Reason, erlang:get_stacktrace()}}
    end.

change(State, {NQMod, QArgs, NVMod, VArgs, NMMod, MArgs},
       #time{now=Now, meter=M} = Time, Q, V,
       #config{queue_mod=QMod, valve_mod=VMod, meter_mod=MMod} = Config) ->
    Inits = [{sbroker_queue, QMod, Q, NQMod, QArgs},
             {sregulator_valve, VMod, {State, V}, NVMod, VArgs},
             {sbroker_meter, MMod, M, NMMod, MArgs}],
    case sbroker_handlers:config_change(Now, Inits, report_name(Config)) of
        {ok, [{_, _, NQ, QNext}, {_, _, {NState, NV}, VNext},
              {_, _, NM, MNext}]} ->
            Next = min(QNext, VNext),
            NTime = Time#time{meter=NM, next=MNext},
            NConfig = Config#config{queue_mod=NQMod, valve_mod=NVMod,
                                    meter_mod=NMMod},
            next(State, NState, NTime, NQ, NV, Next, NConfig);
        {stop, Reason} ->
            exit(Reason)
    end.

next(open, open, Time, Q, V, Next, Config) ->
    open(Time, Q, V, Next, Config);
next(closed, open, #time{send=Open} = Time, Q, V, Next, Config) ->
    opening_out(Time#time{open=Open}, Q, V, Next, Config);
next(_, closed, Time, Q, V, Next, Config) ->
    closed(Time, Q, V, Next, Config).

opening_out(#time{now=Now, open=Open} = Time, Q, V, VNext,
            #config{queue_mod=QMod} = Config) ->
    try QMod:handle_out(Now, Q) of
        {Send, Ask, Pid, Ref, NQ, QNext} ->
            go(Ask, Ref, Send, Open, Now),
            opening_ask(Pid, Ref, Time, NQ, V, QNext, Config);
        {empty, NQ} ->
            open(Time, NQ, V, VNext, Config);
        Other ->
            queue_return(Other, open, Time, Q, V, Config)
    catch
        Class:Reason ->
            queue_exception(Class, Reason, open, Time, Q, V, Config)
    end.

opening_ask(Pid, Ref, #time{now=Now} = Time, Q, V, QNext,
            #config{valve_mod=VMod} = Config) ->
    try VMod:handle_ask(Pid, Ref, Now, V) of
        {open, NV, VNext} ->
            opening_out(Time, Q, NV, VNext, Config);
        {closed, NV, VNext} ->
            closed(Time, Q, NV, min(VNext, QNext), Config);
        Other ->
            valve_return(Other, open, Time, Q, V, Config)
    catch
        Class:Reason ->
            valve_exception(Class, Reason, open, Time, Q, V, Config)
    end.

info_queue(Msg, State, #time{now=Now} = Time, Q, V,
          #config{queue_mod=QMod} = Config) ->
    try QMod:handle_info(Msg, Now, Q) of
        {NQ, QNext} ->
            info_valve(Msg, State, Time, NQ, V, QNext, Config);
        Other ->
            queue_return(Other, State, Time, Q, V, Config)
    catch
        Class:Reason ->
            queue_exception(Class, Reason, State, Time, Q, V, Config)
    end.

info_valve(Msg, State, #time{now=Now} = Time, Q, V, QNext,
          #config{valve_mod=VMod} = Config) ->
    try VMod:handle_info(Msg, Now, V) of
        {NState, NV, VNext} ->
            Next = min(VNext, QNext),
            info_meter(Msg, State, NState, Time, Q, NV, Next, Config);
        Other ->
            valve_return(Other, State, Time, Q, V, Config)
    catch
        Class:Reason ->
            valve_exception(Class, Reason, State, Time, Q, V, Config)
    end.

info_meter(Msg, State, NState, #time{now=Now, meter=M} = Time, Q, V, Next,
           #config{meter_mod=MMod} = Config) ->
    try MMod:handle_info(Msg, Now, M) of
        {NM, MNext} ->
            NTime = Time#time{meter=NM, next=MNext},
            next(State, NState, NTime, Q, V, Next, Config);
        Other ->
            meter_return(Other, NState, Time, Q, V, Config)
    catch
        Class:Reason ->
            meter_exception(Class, NState, Reason, Time, Q, V, Config)
    end.

timeout(open, open, Time, Q, V, Next, Config) ->
    open(Time, Q, V, Next, Config);
timeout(closed, open, #time{send=Open} = Time, Q, V, Next, Config) ->
    opening_out(Time#time{open=Open}, Q, V, Next, Config);
timeout(open, closed, Time, Q, V, Next, Config) ->
    closed(Time, Q, V, Next, Config);
timeout(closed, closed, Time, Q, V, Next, Config) ->
    queue_timeout(Time, Q, V, Next, Config).

timeout(open, Time, Q, V, Config) ->
    open_timeout(Time, Q, V, Config);
timeout(closed, Time, Q, V, Config) ->
    closed_timeout(Time, Q, V, Config).

open_timeout(#time{now=Now} = Time, Q, V, #config{valve_mod=VMod} = Config) ->
    try VMod:handle_timeout(Now, V) of
        {open, NV, Next} ->
            open(Time, Q, NV, Next, Config);
        {closed, NV, Next} ->
            closed(Time, Q, NV, Next, Config);
        Other ->
            valve_return(Other, open, Time, Q, V, Config)
    catch
        Class:Reason ->
            valve_exception(Class, Reason, open, Time, Q, V, Config)
    end.

closed_timeout(#time{now=Now, send=Open} = Time, Q, V,
               #config{valve_mod=VMod} = Config) ->
    try VMod:handle_timeout(Now, V) of
        {closed, NV, VNext} ->
            queue_timeout(Time, Q, NV, VNext, Config);
        {open, NV, VNext} ->
            opening_out(Time#time{open=Open}, Q, NV, VNext, Config);
        Other ->
            valve_return(Other, closed, Time, Q, V, Config)
    catch
        Class:Reason ->
            valve_exception(Class, Reason, closed, Time, Q, V, Config)
    end.

valve_timeout(#time{now=Now, send=Open} = Time, Q, V, QNext,
              #config{valve_mod=VMod} = Config) ->
    try VMod:handle_timeout(Now, V) of
        {closed, NV, VNext} ->
            closed(Time, Q, NV, min(VNext, QNext), Config);
        {open, NV, VNext} ->
            opening_out(Time#time{open=Open}, Q, NV, VNext, Config);
        Other ->
            valve_return(Other, closed, Time, Q, V, Config)
    catch
        Class:Reason ->
            valve_exception(Class, Reason, closed, Time, Q, V, Config)
    end.

queue_timeout(#time{now=Now} = Time, Q, V, VNext,
              #config{queue_mod=QMod} = Config) ->
    try QMod:handle_timeout(Now, Q) of
        {NQ, QNext} ->
            closed(Time, NQ, V, min(QNext, VNext), Config);
        Other ->
            queue_return(Other, closed, Time, Q, V, Config)
    catch
        Class:Reason ->
            queue_exception(Class, Reason, closed, Time, Q, V, Config)
    end.

queue_return(Return, State, Time, Q, V,
             #config{queue_mod=QMod, valve_mod=VMod} = Config) ->
    Reason = {bad_return_value, Return},
    Callbacks = [{sbroker_queue, QMod, Reason, Q},
                 {sregulator_valve, VMod, stop, {State, V}}],
    terminate(Reason, Time, Callbacks, Config).

queue_exception(Class, Reason, State, Time, Q, V,
                #config{queue_mod=QMod, valve_mod=VMod} = Config) ->
    Reason2 = {Class, Reason, erlang:get_stacktrace()},
    Callbacks = [{sbroker_queue, QMod, Reason2, Q},
                 {sregulator_valve, VMod, stop, {State, V}}],
    terminate(Reason2, Time, Callbacks, Config).

valve_return(Return, State, Time, Q, V,
             #config{queue_mod=QMod, valve_mod=VMod} = Config) ->
    Reason = {bad_return_value, Return},
    Callbacks = [{sbroker_queue, QMod, stop, Q},
                 {sregulator_valve, VMod, Reason, {State, V}}],
    terminate(Reason, Time, Callbacks, Config).

valve_exception(Class, Reason, State, Time, Q, V,
                #config{queue_mod=QMod, valve_mod=VMod} = Config) ->
    Reason2 = {Class, Reason, erlang:get_stacktrace()},
    Callbacks = [{sbroker_queue, QMod, stop, Q},
                 {sregulator_valve, VMod, Reason2, {State, V}}],
    terminate(Reason2, Time, Callbacks, Config).

meter_return(Return, State, #time{meter=M}, Q, V,
             #config{queue_mod=QMod, valve_mod=VMod,
                     meter_mod=MMod} = Config) ->
    Reason = {bad_return_value, Return},
    Callbacks = [{sbroker_queue, QMod, stop, Q},
                 {sregulator_valve, VMod, stop, {V, State}},
                 {sbroker_meter, MMod, Reason, M}],
    terminate(Reason, Callbacks, Config).

meter_exception(Class, Reason, State, #time{meter=M}, Q, V,
                #config{queue_mod=QMod, valve_mod=VMod,
                        meter_mod=MMod} = Config) ->
    Reason2 = {Class, Reason, erlang:get_stacktrace()},
    Callbacks = [{sbroker_queue, QMod, stop, Q},
                 {sregulator_valve, VMod, stop, {State, V}},
                 {sbroker_meter, MMod, Reason2, M}],
    terminate(Reason2, Callbacks, Config).

terminate(Reason, Callbacks, Config) ->
    Name = report_name(Config),
    {stop, NReason} = sbroker_handlers:terminate(Reason, Callbacks, Name),
    exit(NReason).

terminate(Reason, #time{meter=Meter}, Callbacks,
          #config{meter_mod=MeterMod} = Config) ->
    NCallbacks = Callbacks ++ [{sbroker_meter, MeterMod, stop, Meter}],
    terminate(Reason, NCallbacks, Config).

terminate(Reason, State, Time, Q, V,
          #config{queue_mod=QMod, valve_mod=VMod} = Config) ->
    Callbacks = [{sbroker_queue, QMod, stop, Q},
                 {sregulator_valve, VMod, stop, {State, V}}],
    terminate(Reason, Time, Callbacks, Config).

report_name(#config{name=Pid, mod=Mod}) when is_pid(Pid) ->
    {Mod, Pid};
report_name(#config{name=Name}) ->
    Name.
