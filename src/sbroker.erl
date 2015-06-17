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
%% @doc
%% This module provides a process match making service. A process joins one of
%% two queues and is matcheid with a process in the other queue. The queues can
%% be actively managed using an `squeue' callback module, and passively managed
%% using head or tail drop. A different strategy can be used for both queues.
%% Processes that die while in a queue are automatically removed to prevent
%% matching with a process that is nolonger alive.
%%
%% There are two functions to join a queue: `ask/1' and `ask_r/1'. Processes
%% that call `ask/1' are matched against processes that call `ask_r/1'. If no
%% match is immediately avaliable a process is queued in the relevant queue
%% until a match becomes avaliable. If queue management is used processes may be
%% dropped without a match.
%%
%% Processes calling `ask/1' try to match with/dequeue a process in the `ask_r'
%% queue. If no process exists they are queued in the `ask' queue and await a
%% process to call `ask_r/1'.
%%
%% Similarly processes calling `ask_r/1' try to match with/dequeue a process
%% in the `ask' queue. If no process exists they are queued in the `ask_r' queue
%% and await a process to call `ask/1'.
%%
%% A broker requires a callback module. The callback modules implements one
%% callback, `init/1', with single argument `Args'. `init/1' should return
%% `{ok, {AskQueueSpec, AskRQueueSpec, Timeout})' or `ignore'. `AskQueuSpec' is
%% the queue specification for the `ask' queue and `AskRQueueSpec' is the queue
%% specification for the `ask_r' queue. `Timeout' is the timeout in
%% milliseconds that the active queue is polled when the broker is idle. This
%% ensures that the active queue management strategy is applied even if no
%% processes are enqueued/dequeued. In the case of `ignore' the broker is not
%% started and `start_link' returns `ignore'.
%%
%% A queue specifcation takes the following form: `{Module, Args}'. `Module' is
%% the `sbroker' callback module and `Args' are its arguments.
%%
%% For example:
%%
%% ```
%% -module(sbroker_example).
%%
%% -behaviour(sbroker).
%%
%% -export([start_link/0]).
%% -export([ask/0]).
%% -export([ask_r/1]).
%% -export([init/1]).
%%
%% start_link() ->
%%     sbroker:start_link({local, ?MODULE}, ?MODULE, []).
%%
%% ask() ->
%%     sbroker:ask(?MODULE).
%%
%% ask_r() ->
%%     sbroker:ask_r(?MODULE).
%%
%% init([]) ->
%%     AskQueueSpec = {sbroker_codel_queue, {out, 5, 100, drop_r, 64}},
%%     AskRQueueSpec = {sbroker_timeout_queue, {out_r, 5000, drop, infinity}},
%%     Timeout = 200,
%%     {ok, {AskQueueSpec, AskRQueueSpec, Timeout}}.
%% '''
-module(sbroker).

%% public api

-export([ask/1]).
-export([ask_r/1]).
-export([nb_ask/1]).
-export([nb_ask_r/1]).
-export([async_ask/1]).
-export([async_ask/2]).
-export([async_ask_r/1]).
-export([async_ask_r/2]).

-export([await/2]).
-export([cancel/3]).
-export([change_config/2]).
-export([len/2]).
-export([len_r/2]).
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

%% macros

-ifndef(READ_TIME_AFTER).
-define(READ_TIME_AFTER, 16).
-endif.

%% types

-type broker() :: pid() | atom() | {atom(), node()} | {global, any()}
    | {via, module(), any()}.
-type name() :: {local, atom()} | {global, any()} | {via, module(), any()}.
-type debug_option() ::
    trace | log | {log, pos_integer()} | statistics |
    {log_to_file, file:filename()} | {install, {fun(), any()}}.
-type start_option() ::
    {debug, debug_option()} | {timeout, timeout()} |
    {spawn_opt, [proc_lib:spawn_option()]} | {time_module, module()} |
    {time_unit, sbroker_time:unit()} |
    {read_time_after, non_neg_integer() | infinity}.
-type start_return() :: {ok, pid()} | ignore | {error, any()}.
-type queue_spec() :: {module(), any(), out | out_r,
                       non_neg_integer() | infinity, drop | drop_r}.

-export_type([broker/0]).
-export_type([queue_spec/0]).

-callback init(Args :: any()) ->
    {ok, {queue_spec(), queue_spec(), pos_integer()}} | ignore.

-record(config, {mod :: module(),
                 args :: any(),
                 parent :: pid(),
                 dbg :: [sys:dbg_opt()],
                 name :: name() | pid(),
                 time_mod :: module(),
                 time_unit :: sbroker_time:unit(),
                 read_time_after :: non_neg_integer() | infinity,
                 ask_mod :: module(),
                 bid_mod :: module(),
                 timeout :: timeout()}).

-dialyzer(no_return).

%% public api

%% @doc Tries to match with a process calling `ask_r/1' on the same broker.
%% Returns `{go, Ref, Pid, RelativeTime, SojournTime}' on a successful match
%% or `{drop, SojournTime}'.
%%
%% `Ref' is the transaction reference, which is a `reference()'. `Pid' is the
%% matched process. `RelativeTime' is the time (in the broker's time unit) spent
%% waiting for a match after discounting time spent waiting for the broker to
%% handle requests. `SojournTime' is the time spent in both the broker's message
%% queue and internal queue.
%%
%% `RelativeTime' represents the `SojournTime' without the overhead of the
%% broker. The value measures the level of queue congestion without being
%% effected by the load of the broker.
%%
%% If `RelativeTime' is positive, the request was enqueued in the internal
%% queue awaiting a match with another request sent approximately `RelativeTime'
%% after this request was sent. Therefore `SojournTime' minus `RelativeTime'
%% is the latency, or overhead, of the broker.
%%
%% If `RelativeTime' is negative, the request dequeued a request in the internal
%% queue that was sent approximately `RelativeTime' before this request was
%% sent. Therefore `SojournTime' is the latency, or overhead, of the broker.
%%
%% If `RelativeTime' is `0', the request was matched with a request sent at
%% approximately the same time. Therefore `SojournTime' is the latency, or
%% overhead, of the broker.
%%
%% The sojourn time for `Pid' can be approximated by `SojournTime' minus
%% `RelativeTime'.
-spec ask(Broker) -> Go | Drop when
      Broker :: broker(),
      Go :: {go, Ref, Pid, RelativeTime, SojournTime},
      Ref :: reference(),
      Pid :: pid(),
      RelativeTime :: integer(),
      SojournTime :: non_neg_integer(),
      Drop :: {drop, SojournTime}.
ask(Broker) ->
    call(Broker, ask, undefined, infinity).

%% @doc Tries to match with a process calling `ask/1' on the same broker.
%%
%% @see ask/1
-spec ask_r(Broker) -> Go | Drop when
      Broker :: broker(),
      Go :: {go, Ref, Pid, RelativeTime, SojournTime},
      Ref :: reference(),
      Pid :: pid(),
      RelativeTime :: integer(),
      SojournTime :: non_neg_integer(),
      Drop :: {drop, SojournTime}.
ask_r(Broker) ->
    call(Broker, bid, undefined, infinity).

%% @doc Tries to match with a process calling `ask_r/1' on the same broker but
%% does not enqueue the request if no immediate match. Returns
%% `{go, Ref, Pid, RelativeTime, SojournTime}' on a successful match or
%% `{retry, SojournTime}'.
%%
%% `Ref' is the transaction reference, which is a `reference()'. `Pid' is the
%% matched process. `RelativeTime' is the time spent waiting for a match after
%% discounting time spent waiting for the broker to handle requests.
%% `SojournTime' is the time spent in the broker's message queue.
%%
%% @see ask/1
-spec nb_ask(Broker) -> Go | Retry when
      Broker :: broker(),
      Go :: {go, Ref, Pid, RelativeTime, SojournTime},
      Ref :: reference(),
      Pid :: pid(),
      RelativeTime :: 0 | neg_integer(),
      SojournTime :: non_neg_integer(),
      Retry :: {retry, SojournTime}.
nb_ask(Broker) ->
    call(Broker, nb_ask, undefined, infinity).

%% @doc Tries to match with a process calling `ask/1' on the same broker but
%% does not enqueue the request if no immediate match.
%%
%% @see nb_ask/1
-spec nb_ask_r(Broker) -> Go | Retry when
      Broker :: broker(),
      Go :: {go, Ref, Pid, RelativeTime, SojournTime},
      Ref :: reference(),
      Pid :: pid(),
      RelativeTime :: 0 | neg_integer(),
      SojournTime :: non_neg_integer(),
      Retry :: {retry, SojournTime}.
nb_ask_r(Broker) ->
    call(Broker, nb_bid, undefined, infinity).

%% @doc Monitors the broker and sends an asynchronous request to match with a
%% process calling `ask_r/1'. Returns `{await, Tag, Pid}'.
%%
%% `Tag' is a monitor `reference()' that uniquely identifies the reply
%% containing the result of the request. `Pid', is the pid (`pid()') of the
%% monitored broker. To cancel the request call `cancel(Pid, Tag)'.
%%
%% The reply is of the form `{Tag, {go, Ref, Pid, RelativeTime, SojournTime}'
%% or `{Tag, {drop, SojournTime}}'.
%%
%% Multiple asynchronous requests can be made from a single process to a
%% broker and no guarantee is made of the order of replies. A process making
%% multiple requests can reuse the monitor reference for subsequent requests to
%% the same broker process (`Process') using `async_ask/2'.
%%
%% @see cancel/2
%% @see async_ask/2
-spec async_ask(Broker) -> {await, Tag, Pid} when
      Broker :: broker(),
      Tag :: reference(),
      Pid :: pid().
async_ask(Broker) ->
    async_call(Broker, ask, undefined).

%% @doc Sends an asynchronous request to match with a process calling `ask_r/1'.
%% Returns `{await, Tag, Pid}'.
%%
%% `Tag' is a `any()' that identifies the reply containing the result of the
%% request. `Pid', is the pid (`pid()') of the broker. To cancel all requests
%% identified by `Tag' on broker `Pid' call `cancel(Pid, Tag)'.
%%
%% The reply is of the form `{Tag, {go, Ref, Pid, RelativeTime, SojournTime}' or
%% `{Tag, {drop, SojournTime}}'.
%%
%% Multiple asynchronous requests can be made from a single process to a
%% broker and no guarantee is made of the order of replies. If the broker
%% exits or is on a disconnected node there is no guarantee of a reply and so
%% the caller should take appropriate steps to handle this scenario.
%%
%% @see cancel/2
-spec async_ask(Broker, Tag) -> {await, Tag, Pid} when
      Broker :: broker(),
      Tag :: any(),
      Pid :: pid().
async_ask(Broker, Tag) ->
    async_call(Broker, ask, undefined, Tag).

%% @doc Monitors the broker and sends an asynchronous request to match with a
%% process calling `ask/1'.
%%
%% @see async_ask/1
%% @see cancel/2
-spec async_ask_r(Broker) -> {await, Tag, Pid} when
      Broker :: broker(),
      Tag :: reference(),
      Pid :: pid().
async_ask_r(Broker) ->
    async_call(Broker, bid, undefined).

%% @doc Sends an asynchronous request to match with a process calling `ask/1'.
%%
%% @see async_ask/2
%% @see cancel/2
-spec async_ask_r(Broker, Tag) -> {await, Tag, Pid} when
      Broker :: broker(),
      Tag :: any(),
      Pid :: pid().
async_ask_r(Broker, Tag) ->
    async_call(Broker, bid, undefined, Tag).

%% @doc Await the response to an asynchronous request identified by `Tag'.
%%
%% Exits if a response is not received after `Timeout' milliseconds.
%%
%% Exits if a `DOWN' message is received with the reference `Tag'.
%%
%% @see async_ask/2
%% @see async_ask_r/2
-spec await(Tag, Timeout) -> Go | Drop when
      Tag :: any(),
      Timeout :: timeout(),
      Go :: {go, Ref, Pid, RelativeTime, SojournTime},
      Ref :: reference(),
      Pid :: pid(),
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

%% @doc Cancels an asynchronous request. Returns the number of cancelled
%% requests or `false' if no requests exist. In the later case a caller may wish
%% to check its message queue for an existing reply.
%%
%% @see async_ask/1
%% @see async_ask_r/1
-spec cancel(Broker, Tag, Timeout) -> Count | false when
      Broker :: broker(),
      Tag :: any(),
      Timeout :: timeout(),
      Count :: pos_integer().
cancel(Broker, Tag, Timeout) ->
    call(Broker, cancel, Tag, Timeout).

%% @doc Change the configuration of the broker. Returns `ok' on success and
%% `{error, Reason}' on failure, where `Reason', is the reason for failure.
%%
%% Broker calls the `init/1' callback to get the new configuration. If `init/1'
%% returns `ignore' the config does not change.
-spec change_config(Broker, Timeout) -> ok | {error, Reason} when
      Broker :: broker(),
      Timeout :: timeout(),
      Reason :: any().
change_config(Broker, Timeout) ->
    call(Broker, change_config, undefined, Timeout).

%% @doc Get the length of the `ask' queue in the broker, `Broker'.
-spec len(Broker, Timeout) -> Length when
      Broker :: broker(),
      Timeout :: timeout(),
      Length :: non_neg_integer().
len(Broker, Timeout) ->
    call(Broker, len_ask, undefined, Timeout).

%% @doc Get the length of the `ask_r' queue in the broker, `Broker'.
-spec len_r(Broker, Timeout) -> Length when
      Broker :: broker(),
      Timeout :: timeout(),
      Length :: non_neg_integer().
len_r(Broker, Timeout) ->
    call(Broker, len_bid, undefined, Timeout).

%% @doc Starts a broker with callback module `Module' and argument `Args', and
%% broker options `Opts'.
%%
%% `Opts' is a `proplist' and supports `debug', `timeout' and `spawn_opt' used
%% by `gen_server' and `gen_fsm'. `time_module' sets the `sbroker_time'
%% callback module, which defaults to `erlang' if `erlang:monotonic_time/1' is
%% exported, otherwise `sbroker_legacy', which uses `erlang:now/0'. The time
%% units are set with `time_unit', which defaults to `native'. `read_time_after'
%% sets the number of requests when a cached time is stale and the time is read
%% again. Its value is a `non_neg_integer()' or `infinity' and defaults to `16'.
%%
%% @see gen_server:start_link/3
%% @see sbroker_time
-spec start_link(Module, Args, Opts) -> StartReturn when
      Module :: module(),
      Args :: any(),
      Opts :: [start_option()],
      StartReturn :: start_return().
start_link(Mod, Args, Opts) ->
    gen:start(?MODULE, link, Mod, Args, Opts).

%% @doc Starts a broker with name `Name', callback module `Module' and argument
%% `Args', and broker options `Opts'.
%%
%% @see start_link/3
-spec start_link(Name, Module, Args, Opts) -> StartReturn when
      Name :: name(),
      Module :: module(),
      Args :: any(),
      Opts :: [start_option()],
      StartReturn :: start_return().
start_link(Name, Mod, Args, Opts) ->
    gen:start(?MODULE, link, Name, Mod, Args, Opts).

%% test api

%% @hidden
-spec timeout(Broker) -> ok when
      Broker :: broker().
timeout(Broker) ->
    send(Broker, timeout).

%% gen_fsm api

%% Inside the gen_fsm an ask_r request is refered to as a bid to make the
%% difference between ask and ask_r clearer.

%% @private
init_it(Starter, self, Name, Mod, Args, Opts) ->
    init_it(Starter, self(), Name, Mod, Args, Opts);
init_it(Starter, Parent, Name, Mod, Args, Opts) ->
    DbgOpts = proplists:get_value(debug, Opts, []),
    Dbg = sys:debug_options(DbgOpts),
    {TimeMod, TimeUnit, ReadAfter} = time_options(Opts),
    _ = put('$initial_call', {Mod, init, 1}),
    try Mod:init(Args) of
        {ok, {{AskMod, AskArgs}, {BidMod, BidArgs}, Timeout}} ->
            Config = #config{mod=Mod, args=Args, parent=Parent, dbg=Dbg,
                             name=Name, time_mod=TimeMod, time_unit=TimeUnit,
                             read_time_after=ReadAfter, ask_mod=AskMod,
                             bid_mod=BidMod, timeout=Timeout},
            init_asks(Starter, AskArgs, BidArgs, Config);
        ignore ->
            init_stop(Starter, Name, ignore, normal);
        Other ->
            Reason = {bad_return_value, Other},
            init_stop(Starter, Name, Reason)
    catch
        Class:Reason ->
            Stack = erlang:get_stacktrace(),
            init_exception(Starter, Name, Class, Reason, Stack)
    end.

%% sys API

%% @private
system_continue(Parent, Dbg, [State, _, Send, Asks, Bids, Config]) ->
    Time = monotonic_time(Config),
    NConfig = Config#config{parent=Parent, dbg=Dbg},
    case State of
        asking ->
            asking_timeout(Time, Send, 0, Asks, Bids, NConfig);
        bidding ->
            bidding_timeout(Time, Send, 0, Asks, Bids, NConfig)
    end.

%% @private
system_code_change(Misc, _, _, _) ->
    {ok, Misc}.

%% @private
system_get_state([_, _, _, Asks, Bids,
                  #config{ask_mod=AskMod, bid_mod=BidMod}]) ->
    Callbacks = [{AskMod, ask_queue, Asks},
                 {BidMod, ask_r_queue, Bids}],
    {ok, Callbacks}.

%% @private
system_replace_state(Replace,
                     [State, Time, Send, Asks, Bids,
                      #config{ask_mod=AskMod, bid_mod=BidMod} = Config]) ->
    {AskMod, ask_queue, NAsks} = AskRes = Replace({AskMod, ask_queue, Asks}),
    BidRes = Replace({BidMod, ask_r_queue, Bids}),
    {BidMod, ask_r_queue, NBids} = BidRes,
    {ok, [AskRes, BidRes], [State, Time, Send, NAsks, NBids, Config]}.

%% @private
system_terminate(Reason, _, _, _) ->
    exit(Reason).

%% Internal

call(Broker, Label, Msg, Timeout) ->
    case sbroker_util:whereis(Broker) of
        undefined ->
            exit({noproc, {?MODULE, call, [Broker, Label, Msg, Timeout]}});
        Process ->
            try gen:call(Process, Label, Msg, Timeout) of
                {ok, Reply} ->
                    Reply
            catch
                exit:Reason ->
                    Args = [Broker, Label, Msg, Timeout],
                    exit({Reason, {?MODULE, call, Args}})
            end
    end.

async_call(Broker, Label, Msg) ->
    case sbroker_util:whereis(Broker) of
         undefined ->
            exit({noproc, {?MODULE, async_call, [Broker, Label, Msg]}});
        Process ->
            Tag = monitor(process, Process),
            _ = Process ! {Label, {self(), Tag}, Msg},
            {await, Tag, Process}
    end.

async_call(Broker, Label, Msg, Tag) ->
    case sbroker_util:whereis(Broker) of
         undefined ->
            exit({noproc, {?MODULE, async_call, [Broker, Label, Msg, Tag]}});
        Process ->
            _ = Process ! {Label, {self(), Tag}, Msg},
            {await, Tag, Process}
    end.

send(Broker, Msg) ->
    case sbroker_util:whereis(Broker) of
        undefined ->
            exit({noproc, {?MODULE, send, [Broker, Msg]}});
        Process ->
            _ = Process ! Msg,
            ok
    end.

time_options(Opts) ->
    TimeMod = proplists:get_value(time_module, Opts, time_module()),
    TimeUnit = proplists:get_value(time_unit, Opts, native),
    ReadAfter = proplists:get_value(read_time_after, Opts, ?READ_TIME_AFTER),
    {TimeMod, TimeUnit, ReadAfter}.

time_module() ->
    case erlang:function_exported(erlang, monotonic_time, 1) of
        true  -> erlang;
        false -> sbroker_legacy
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

init_asks(Starter, AskArgs, BidArgs, #config{ask_mod=AskMod} = Config) ->
    Time = monotonic_time(Config),
    try AskMod:init(Time, AskArgs) of
        Asks ->
            init_bids(Starter, Time, Asks, BidArgs, Config)
    catch
        Class:Reason ->
            Stack = erlang:get_stacktrace(),
            init_exception(Starter, Class, Reason, Stack, [], Config)
    end.

init_bids(Starter, Time, Asks, BidArgs,
          #config{bid_mod=BidMod, ask_mod=AskMod} = Config) ->
    try BidMod:init(Time, BidArgs) of
        Bids ->
            enter_loop(Starter, Asks, Bids, Config)
    catch
        Class:Reason ->
            Stack = erlang:get_stacktrace(),
            Callbacks = [{AskMod, stop, Asks}],
            init_exception(Starter, Class, Reason, Stack, Callbacks, Config)
    end.

init_exception(Starter, Name, Class, Reason, Stack) ->
    Reason2 = reason({Class, Reason, Stack}),
    init_stop(Starter, Name, {error, Reason2}, Reason2).

init_exception(Starter, Class, Reason, Stack, Callbacks,
               #config{name=Name} = Config) ->
    Reason2 = {Class, Reason, Stack},
    NReason = terminate(Reason2, Callbacks, Config),
    NReason2 = reason(NReason),
    init_stop(Starter, Name, {error, NReason2}, NReason2).

enter_loop(Starter, Asks, Bids, Config) ->
    proc_lib:init_ack(Starter, {ok, self()}),
    asking_idle(Asks, Bids, Config).

monotonic_time(#config{time_mod=TimeMod, time_unit=native}) ->
    TimeMod:monotonic_time();
monotonic_time(#config{time_mod=TimeMod, time_unit=TimeUnit}) ->
    TimeMod:monotonic_time(TimeUnit).

mark(Time, 0, _) ->
    _ = self() ! {'$mark', Time},
    Time;
mark(_, _, Config) ->
    mark(Config).

mark(Config) ->
    Time = monotonic_time(Config),
    _ = self() ! {'$mark', Time},
    Time.

asking_idle(Asks, Bids, #config{timeout=Timeout} = Config) ->
    receive
        Msg ->
            Now = mark(Config),
            asking(Msg, Now, Now, 0, Asks, Bids, Config)
    after
        Timeout ->
            Now = mark(Config),
            asking_timeout(Now, Now, 0, Asks, Bids, Config)
    end.

asking_timeout(Now, Send, Seq, Asks, Bids, #config{ask_mod=AskMod} = Config) ->
    try AskMod:handle_timeout(Now, Asks) of
        NAsks ->
            asking(Now, Send, Seq, NAsks, Bids, Config)
    catch
        Class:Reason ->
            asking_exception(Class, Reason, Asks, Bids, Config)
    end.

asking(_, Send, ReadAfter, Asks, Bids,
       #config{read_time_after=ReadAfter} = Config) ->
    receive
        Msg ->
            Now = monotonic_time(Config),
            asking(Msg, Now, Send, 0, Asks, Bids, Config)
    end;
asking(Now, Send, Seq, Asks, Bids, Config) ->
    receive
        Msg ->
            asking(Msg, Now, Send, Seq + 1, Asks, Bids, Config)
    end.

asking({ask, Ask, _}, Now, Send, Seq, Asks, Bids,
       #config{ask_mod=AskMod} = Config) ->
    try AskMod:handle_in(Send, Ask, Now, Asks) of
        NAsks ->
            asking(Now, Send, Seq, NAsks, Bids, Config)
    catch
        Class:Reason ->
            asking_exception(Class, Reason, Asks, Bids, Config)
    end;
asking({bid, Bid, _} = Msg, Now, Send, Seq, Asks, Bids,
       #config{ask_mod=AskMod} = Config) ->
    try AskMod:handle_out(Now, Asks) of
        {AskSojourn, Ask, NAsks} ->
            settle(Now, Now - AskSojourn, Ask, Send, Bid),
            asking(Now, Send, Seq, NAsks, Bids, Config);
        {empty, NAsks} ->
            bidding(Msg, Now, Send, Seq, NAsks, Bids, Config);
        Other ->
            asking_return(Other, Asks, Bids, Config)
    catch
        Class:Reason ->
            asking_exception(Class, Reason, Asks, Bids, Config)
    end;
asking({nb_ask, Ask, _}, Now, Send, Seq, Asks, Bids, Config) ->
    retry(Ask, Now, Send),
    asking_timeout(Now, Send, Seq, Asks, Bids, Config);
asking({nb_bid, Bid, _}, Now, Send, Seq, Asks, Bids,
       #config{ask_mod=AskMod} = Config) ->
    try AskMod:handle_out(Now, Asks) of
        {AskSojourn, Ask, NAsks} ->
            settle(Now, Now - AskSojourn, Ask, Send, Bid),
            asking(Now, Send, Seq, NAsks, Bids, Config);
        {empty, NAsks} ->
            retry(Bid, Now, Send),
            asking_timeout(Now, Send, Seq, NAsks, Bids, Config);
        Other ->
            asking_return(Other, Asks, Bids, Config)
    catch
        Class:Reason ->
            asking_exception(Class, Reason, Asks, Bids, Config)
    end;
asking({cancel, From, Tag}, Now, Send, Seq, Asks, Bids,
       #config{ask_mod=AskMod} = Config) ->
    try AskMod:handle_cancel(Tag, Now, Asks) of
        {Reply, NAsks} ->
            gen:reply(From, Reply),
            asking(Now, Send, Seq, NAsks, Bids, Config);
        Other ->
            asking_return(Other, Asks, Bids, Config)
    catch
        Class:Reason ->
            asking_exception(Class, Reason, Asks, Bids, Config)
    end;
asking({'$mark', Mark}, Time, _, Seq, Asks, Bids, Config) ->
    receive
        Msg ->
            Now = mark(Time, Seq, Config),
            Send = (Mark + Now) div 2,
            asking(Msg, Now, Send, 0, Asks, Bids, Config)
    after
        0 ->
            asking_idle(Asks, Bids, Config)
    end;
asking({'EXIT', Parent, Reason}, _, _, _, _, _, #config{parent=Parent}) ->
    exit(Reason);
asking({system, From, Msg}, Now, Send, _, Asks, Bids, Config) ->
    system(From, Msg, asking, Now, Send, Asks, Bids, Config);
asking({change_config, From, _}, Now, Send, Seq, Asks, Bids, Config) ->
    config_change(From, asking, Now, Send, Seq, Asks, Bids, Config);
asking({len_ask, From, _}, Now, Send, Seq, Asks, Bids,
       #config{ask_mod=AskMod} = Config) ->
    try AskMod:len(Asks) of
        Len ->
            gen:reply(From, Len),
            asking_timeout(Now, Send, Seq, Asks, Bids, Config)
    catch
        Class:Reason ->
            asking_exception(Class, Reason, Asks, Bids, Config)
    end;
asking({len_bid, From, _}, Now, Send, Seq, Asks, Bids, Config) ->
    gen:reply(From, 0),
    asking_timeout(Now, Send, Seq, Asks, Bids, Config);
asking(timeout, Now, Send, Seq, Asks, Bids, Config) ->
    asking_timeout(Now, Send, Seq, Asks, Bids, Config);
asking(Msg, Now, Send, Seq, Asks, Bids, Config) ->
    info_asks(Msg, asking, Now, Send, Seq, Asks, Bids, Config).

info_asks(Msg, State, Now, Send, Seq, Asks, Bids,
          #config{ask_mod=AskMod} = Config) ->
    try AskMod:handle_info(Msg, Now, Asks) of
        NAsks ->
            info_bids(Msg, State, Now, Send, Seq, NAsks, Bids, Config)
    catch
        Class:Reason ->
            asking_exception(Class, Reason, Asks, Bids, Config)
    end.

info_bids(Msg, State, Now, Send, Seq, Asks, Bids,
          #config{bid_mod=BidMod} = Config) ->
    try BidMod:handle_info(Msg, Now, Bids) of
        NBids when State =:= asking ->
            asking(Now, Send, Seq, Asks, NBids, Config);
        NBids when State =:= bidding ->
            bidding(Now, Send, Seq, Asks, NBids, Config)
    catch
        Class:Reason ->
            bidding_exception(Class, Reason, Asks, Bids, Config)
    end.

asking_return(Return, Asks, Bids,
              #config{ask_mod=AskMod, bid_mod=BidMod} = Config) ->
    Reason = {bad_return_value, Return},
    Callbacks = [{AskMod, Reason, Asks},
                 {BidMod, stop, Bids}],
    terminate(Reason, Callbacks, Config).

asking_exception(Class, Reason, Asks, Bids,
                 #config{ask_mod=AskMod, bid_mod=BidMod} = Config) ->
    Reason2 = {Class, Reason, erlang:get_stacktrace()},
    Callbacks = [{AskMod, Reason2, Asks},
                 {BidMod, stop, Bids}],
    terminate(Reason2, Callbacks, Config).

bidding_idle(Asks, Bids, #config{timeout=Timeout} = Config) ->
    receive
        Msg ->
            Now = mark(Config),
            bidding(Msg, Now, Now, 0, Asks, Bids, Config)
    after
        Timeout ->
            Now = mark(Config),
            bidding_timeout(Now, Now, 0, Asks, Bids, Config)
    end.

bidding_timeout(Now, Send, Seq, Asks, Bids, #config{bid_mod=BidMod} = Config) ->
    try BidMod:handle_timeout(Now, Bids) of
        NBids ->
            bidding(Now, Send, Seq, Asks, NBids, Config)
    catch
        Class:Reason ->
            bidding_exception(Class, Reason, Asks, Bids, Config)
    end.

bidding(_, Send, ReadAfter, Asks, Bids,
        #config{read_time_after=ReadAfter} = Config) ->
    receive
        Msg ->
            Now = monotonic_time(Config),
            bidding(Msg, Now, Send, 0, Asks, Bids, Config)
    end;
bidding(Now, Send, Seq, Asks, Bids, Config) ->
    receive
        Msg ->
            bidding(Msg, Now, Send, Seq + 1, Asks, Bids, Config)
    end.

bidding({bid, Bid, _}, Now, Send, Seq, Asks, Bids,
        #config{bid_mod=BidMod} = Config) ->
    try BidMod:handle_in(Send, Bid, Now, Bids) of
        NBids ->
            bidding(Now, Send, Seq, Asks, NBids, Config)
    catch
        Class:Reason ->
            bidding_exception(Class, Reason, Asks, Bids, Config)
    end;
bidding({ask, Ask, _} = Msg, Now, Send, Seq, Asks, Bids,
        #config{bid_mod=BidMod} = Config) ->
    try BidMod:handle_out(Now, Bids) of
        {BidSojourn, Bid, NBids} ->
            settle(Now, Send, Ask, Now - BidSojourn, Bid),
            bidding(Now, Send, Seq, Asks, NBids, Config);
        {empty, NBids} ->
            asking(Msg, Now, Send, Seq, Asks, NBids, Config);
        Other ->
            bidding_return(Other, Asks, Bids, Config)
    catch
        Class:Reason ->
            bidding_exception(Class, Reason, Asks, Bids, Config)
    end;
bidding({nb_bid, Bid, _}, Now, Send, Seq, Asks, Bids, Config) ->
    retry(Bid, Now, Send),
    bidding_timeout(Now, Send, Seq, Asks, Bids, Config);
bidding({nb_ask, Ask, _}, Now, Send, Seq, Asks, Bids,
        #config{bid_mod=BidMod} = Config) ->
    try BidMod:handle_out(Now, Bids) of
        {BidSojourn, Bid, NBids} ->
            settle(Now, Send, Ask, Now - BidSojourn, Bid),
            bidding(Now, Send, Seq, Asks, NBids, Config);
        {empty, NBids} ->
            retry(Ask, Now, Send),
            asking_timeout(Now, Send, Seq, Asks, NBids, Config);
        Other ->
            bidding_return(Other, Asks, Bids, Config)
    catch
        Class:Reason ->
            bidding_exception(Class, Reason, Asks, Bids, Config)
    end;
bidding({cancel, From, Tag}, Now, Send, Seq, Asks, Bids,
        #config{bid_mod=BidMod} = Config) ->
    try BidMod:handle_cancel(Tag, Now, Bids) of
        {Reply, NBids} ->
            gen:reply(From, Reply),
            bidding(Now, Send, Seq, Asks, NBids, Config);
        Other ->
            bidding_return(Other, Asks, Bids, Config)
    catch
        Class:Reason ->
            bidding_exception(Class, Reason, Asks, Bids, Config)
    end;
bidding({'$mark', Mark}, Time, _, Seq, Asks, Bids, Config) ->
    receive
        Msg ->
            Now = mark(Time, Seq, Config),
            Send = (Mark + Now) div 2,
            bidding(Msg, Now, Send, 0, Asks, Bids, Config)
    after
        0 ->
            bidding_idle(Asks, Bids, Config)
    end;
bidding({'EXIT', Parent, Reason}, _, _, _, _, _, #config{parent=Parent}) ->
    exit(Reason);
bidding({system, From, Msg}, Now, Send, _, Asks, Bids, Config) ->
    system(From, Msg, bidding, Now, Send, Asks, Bids, Config);
bidding({change_config, From, _}, Now, Send, Seq, Asks, Bids, Config) ->
    config_change(From, bidding, Now, Send, Seq, Asks, Bids, Config);
bidding({len_ask, From, _}, Now, Send, Seq, Asks, Bids, Config) ->
    gen:reply(From, 0),
    bidding_timeout(Now, Send, Seq, Asks, Bids, Config);
bidding({len_bid, From, _}, Now, Send, Seq, Asks, Bids,
        #config{bid_mod=BidMod} = Config) ->
    try BidMod:len(Bids) of
        Len ->
            gen:reply(From, Len),
            bidding_timeout(Now, Send, Seq, Asks, Bids, Config)
    catch
        Class:Reason ->
            bidding_exception(Class, Reason, Asks, Bids, Config)
    end;
bidding(timeout, Now, Send, Seq, Asks, Bids, Config) ->
    bidding_timeout(Now, Send, Seq, Asks, Bids, Config);
bidding(Msg, Now, Send, Seq, Asks, Bids, Config) ->
    info_asks(Msg, bidding, Now, Send, Seq, Asks, Bids, Config).


bidding_return(Return, Asks, Bids,
               #config{ask_mod=AskMod, bid_mod=BidMod} = Config) ->
    Reason = {bad_return_value, Return},
    Callbacks = [{AskMod, stop, Asks},
                 {BidMod, Reason, Bids}],
    terminate(Reason, Callbacks, Config).

bidding_exception(Class, Reason, Asks, Bids,
                  #config{ask_mod=AskMod, bid_mod=BidMod} = Config) ->
    Reason2 = {Class, Reason, erlang:get_stacktrace()},
    Callbacks = [{AskMod, stop, Asks},
                 {BidMod, Reason2, Bids}],
    terminate(Reason2, Callbacks, Config).

settle(Now, BidSend, {BidPid, _} = Bid, AskSend, {AskPid, _} = Ask) ->
    Ref = make_ref(),
    RelativeTime = AskSend - BidSend,
    %% Bid always messaged first.
    gen:reply(Bid, {go, Ref, AskPid, RelativeTime, Now - BidSend}),
    gen:reply(Ask, {go, Ref, BidPid, -RelativeTime, Now - AskSend}).

retry(From, Now, Send) ->
    gen:reply(From, {retry, Now - Send}).

config_change(From, State, Now, Send, Seq, Asks, Bids, Config) ->
    case config_change(Config) of
        {ok, AskMod, AskArgs, BidMod, BidArgs, NConfig} ->
            gen:reply(From, ok),
            {NAsks, NBids, NConfig2} = change_asks(Now, Asks, AskMod, AskArgs,
                                                   Bids, BidMod, BidArgs,
                                                   NConfig),
            next(State, Now, Send, Seq, NAsks, NBids, NConfig2);
        ignore  ->
            gen:reply(From, ok),
            timeout(State, Now, Send, Seq, Asks, Bids, Config);
        {error, Reason} ->
            gen:reply(From, {error, Reason}),
            timeout(State, Now, Send, Seq, Asks, Bids, Config)
    end.

config_change(#config{mod=Mod, args=Args} = Config) ->
    try Mod:init(Args) of
        {ok, {{AskMod, AskArgs}, {BidMod, BidArgs}, Timeout}} ->
            NConfig = Config#config{timeout=Timeout},
            {ok, AskMod, AskArgs, BidMod, BidArgs, NConfig};
        ignore ->
            ignore;
        Other ->
            {error, {bad_return_value, Other}}
    catch
        Class:Reason ->
            {error, {Class, Reason, erlang:get_stacktrace()}}
    end.

change_asks(Now, Asks, NAskMod, AskArgs, Bids, NBidMod, BidArgs,
            #config{ask_mod=AskMod, bid_mod=BidMod} = Config) ->
    case change(Now, AskMod, Asks, NAskMod, AskArgs, Config) of
        {ok, NAsks} ->
            NConfig = Config#config{ask_mod=NAskMod},
            change_bids(Now, NAsks, Bids, NBidMod, BidArgs, NConfig);
        {stop, Reason, Callbacks} ->
            NCallbacks = [{BidMod, stop, Bids} | Callbacks],
            terminate(Reason, Config, NCallbacks)
    end.

change_bids(Now, Asks, Bids, NBidMod, BidArgs,
            #config{bid_mod=BidMod, ask_mod=AskMod} = Config) ->
    case change(Now, BidMod, Bids, NBidMod, BidArgs, Config) of
        {ok, NBids} ->
            {Asks, NBids, Config#config{bid_mod=NBidMod}};
        {stop, Reason, Callbacks} ->
            NCallbacks = [{AskMod, stop, Asks} | Callbacks],
            terminate(Reason, NCallbacks, Config)
    end.

change(Now, Mod, State, Mod, Args, _) ->
    try Mod:config_change(Args, Now, State) of
        NState ->
            {ok, NState}
    catch
        Class:Reason ->
            Reason2 = {Class, Reason, erlang:get_stacktrace()},
            Callbacks = [{Mod, Reason2, State}],
            {stop, Reason2, Callbacks}
    end;
change(Now, Mod1, State1, Mod2, Args2, Config) ->
    try Mod1:to_list(State1) of
        Items when is_list(Items) ->
            change_init(Now, Items, Mod1, State1, Mod2, Args2, Config);
        Other ->
            Reason = {bad_return_value, Other},
            {stop, Reason, [{Mod1, Reason, State1}]}
    catch
        Class:Reason ->
            Reason2 = {Class, Reason, erlang:get_stacktrace()},
            {stop, Reason2, [{Mod1, stop, State1}]}
    end.


change_init(Now, Items, Mod1, State1, Mod2, Args2, Config) ->
    try Mod2:init(Now,Args2) of
        State2 ->
            change_from_list(Items, Now, Mod1, State1, Mod2, State2, Config)
    catch
        Class:Reason ->
            Reason2 = {Class, Reason, erlang:get_stacktrace()},
            change_report(Mod2, Reason2, Args2, Config),
            {stop, Reason2, [{Mod1, stop, State1}]}
    end.

change_report(Mod, Reason, Args, Config) ->
    Tag = {sbroker_queue_stop, start_error},
    Format = "~i** sbroker_queue ~p failed to install.~n"
             "** Was installing in ~p~n"
             "** When queue arguments == ~p~n"
             "** Reason == ~p~n",
    Args = [Tag, Mod, report_name(Config), Args, Reason],
    stop_logger:format(Format, Args).

change_from_list(Items, Now, Mod1, State1, Mod2, State2, Config) ->
    In = fun(Item, Acc) -> change_in(Item, Now, Mod2, Acc) end,
    try lists:foldl(In, State2, Items) of
        NState2 ->
            change_terminate(Mod1, State1, Mod2, NState2, Config)
    catch
        throw:{stop, Reason, Callbacks} ->
            {stop, Reason, [{Mod1, stop, State1} | Callbacks]};
        error:function_clause ->
            Reason = {bad_return_value, Items},
            {stop, Reason, [{Mod1, Reason, State1}, {Mod2, stop, State2}]}
    end.

change_in({Send, From}, Now, Mod, State) ->
    try
        Mod:handle_in(Send, From, Now, State)
    catch
        Class:Reason ->
            Reason2 = {Class, Reason, erlang:get_stacktrace()},
            throw({stop, Reason2, [{Mod, Reason2, State}]})
    end.

change_terminate(Mod1, State1, Mod2, State2, Config) ->
    try Mod1:terminate(change, State1) of
        _ ->
            {ok, State2}
    catch
        Class:Reason ->
            Reason2 = {Class, Reason, erlang:get_stacktrace()},
            report(Mod1, Reason2, State1, Config),
            {error, Reason2, [{Mod2, stop, State2}]}
    end.

next(asking, Now, Send, Seq, Asks, Bids, Config) ->
    asking(Now, Send, Seq, Asks, Bids, Config);
next(bidding, Now, Send, Seq, Asks, Bids, Config) ->
    bidding(Now, Send, Seq, Asks, Bids, Config).

timeout(asking, Now, Send, Seq, Asks, Bids, Config) ->
    asking_timeout(Now, Send, Seq, Asks, Bids, Config);
timeout(bidding, Now, Send, Seq, Asks, Bids, Config) ->
    bidding_timeout(Now, Send, Seq, Asks, Bids, Config).

system(From, Msg, State, Now, Send, Asks, Bids,
       #config{parent=Parent, dbg=Dbg} = Config) ->
    NConfig = Config#config{dbg=[]},
    sys:handle_system_msg(Msg, From, Parent, ?MODULE, Dbg,
                          [State, Now, Send, Asks, Bids, NConfig]).

terminate(Reason, Callbacks, Config) ->
    Terminate = fun(Callback, Acc) -> do_terminate(Callback, Acc, Config) end,
    NReason = lists:foldl(Terminate, Reason, Callbacks),
    Exit = reason(NReason),
    exit(Exit).

do_terminate({Mod, Info, State}, Config, Acc) ->
    try Mod:terminate(Info, State) of
        _ when Info =:= stop ->
            Acc;
        _ ->
            maybe_report(Mod, Info, State, Config),
            Acc
    catch
        Class:Reason ->
            Reason2 = {Class, Reason, erlang:get_stacktrace()},
            report(Mod, Reason2, State, Config),
            Reason2
    end.

reason({throw, Value, Stack}) ->
    {{nocatch, Value}, Stack};
reason({exit, Reason, _}) ->
    Reason;
reason({error, Reason, Stack}) ->
    {Reason, Stack};
reason({bad_return_value, _} = Bad) ->
    Bad.

maybe_report(_, {exit, normal, _}, _, _) ->
    ok;
maybe_report(_, {exit, shutdown, _}, _, _) ->
    ok;
maybe_report(_, {exit, {shutdown, _}, _}, _, _) ->
    ok;
maybe_report(_, {error, shutdown, _}, _, _) ->
    ok;
maybe_report(Mod, Reason, State, Config) ->
    report(Mod, Reason, State, Config).

report(Mod, Reason, State, Config) ->
    Tag = {sbroker_queue_error, queue_crashed},
    Format = "~i** sbroker_queue ~p crashed.~n"
             "** Was installed in ~p~n"
             "** When queue state == ~p~n"
             "** Reason == ~p~n",
    Args = [Tag, Mod, report_name(Config), State, Reason],
    error_logger:format(Format, Args).

report_name(#config{name={local, Name}}) ->
    Name;
report_name(#config{name={global, Name}}) ->
    Name;
report_name(#config{name={via, _, Name}}) ->
    Name;
report_name(#config{name=Pid, mod=Mod}) when is_pid(Pid) ->
    {Mod, Pid}.
