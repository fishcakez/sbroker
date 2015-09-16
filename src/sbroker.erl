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
%% be actively managed using an `sbroker_queue' callback module, and passively
%% managed using head or tail drop. A different strategy can be used for both
%% queues. Processes that die while in a queue are automatically removed to
%% prevent matching with a process that is nolonger alive.
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
%% `{ok, {AskQueueSpec, AskRQueueSpec})' or `ignore'. `AskQueuSpec' is
%% the queue specification for the `ask' queue and `AskRQueueSpec' is the queue
%% specification for the `ask_r' queue. In the case of `ignore' the broker is
%% not started and `start_link' returns `ignore'.
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
%%     {ok, {AskQueueSpec, AskRQueueSpec}}.
%% '''
-module(sbroker).

%% public api

-export([ask/1]).
-export([ask/2]).
-export([ask_r/1]).
-export([ask_r/2]).
-export([nb_ask/1]).
-export([nb_ask/2]).
-export([nb_ask_r/1]).
-export([nb_ask_r/2]).
-export([async_ask/1]).
-export([async_ask/2]).
-export([async_ask/3]).
-export([async_ask_r/1]).
-export([async_ask_r/2]).
-export([async_ask_r/3]).
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
-export([format_status/2]).

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
-type queue_spec() :: {module(), any()}.

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
                 ask_mod :: module(),
                 bid_mod :: module()}).

-record(time, {now :: integer(),
               send :: integer(),
               seq :: non_neg_integer(),
               read_after :: non_neg_integer() | infinity}).

-dialyzer(no_return).

%% public api

%% @equiv ask(Broker, self())
-spec ask(Broker) -> Go | Drop when
      Broker :: broker(),
      Go :: {go, Ref, Value, RelativeTime, SojournTime},
      Ref :: reference(),
      Value :: any(),
      RelativeTime :: integer(),
      SojournTime :: non_neg_integer(),
      Drop :: {drop, SojournTime}.
ask(Broker) ->
    call(Broker, ask, self(), infinity).

%% @doc Send a match request, with value `ReqValue', to try to match with a
%% process calling `ask_r/2' on the broker, `Broker'.
%%
%% Returns `{go, Ref, Value, RelativeTime, SojournTime}' on a successful
%% match or `{drop, SojournTime}'.
%%
%% `Ref' is the transaction reference, which is a `reference()'. `Value' is the
%% value of the matched request sent by the counterparty process. `RelativeTime'
%% is the approximate time differnece (in the broker's time unit) between when
%% the request was sent and the matching request was sent. `SojournTime' is the
%% approximate time spent in both the broker's message queue and internal queue.
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
%% The sojourn time for matched process can be approximated by `SojournTime'
%% minus `RelativeTime'.
-spec ask(Broker, ReqValue) -> Go | Drop when
      Broker :: broker(),
      ReqValue :: any(),
      Go :: {go, Ref, Value, RelativeTime, SojournTime},
      Ref :: reference(),
      Value :: any(),
      RelativeTime :: integer(),
      SojournTime :: non_neg_integer(),
      Drop :: {drop, SojournTime}.
ask(Broker, ReqValue) ->
    call(Broker, ask, ReqValue, infinity).

%% @equiv ask_r(Broker, self())
-spec ask_r(Broker) -> Go | Drop when
      Broker :: broker(),
      Go :: {go, Ref, Value, RelativeTime, SojournTime},
      Ref :: reference(),
      Value ::  any(),
      RelativeTime :: integer(),
      SojournTime :: non_neg_integer(),
      Drop :: {drop, SojournTime}.
ask_r(Broker) ->
    call(Broker, bid, self(), infinity).

%% @doc Tries to match with a process calling `ask/2' on the same broker.
%%
%% @see ask/2
-spec ask_r(Broker, ReqValue) -> Go | Drop when
      Broker :: broker(),
      ReqValue :: any(),
      Go :: {go, Ref, Value, RelativeTime, SojournTime},
      Ref :: reference(),
      Value :: any(),
      RelativeTime :: integer(),
      SojournTime :: non_neg_integer(),
      Drop :: {drop, SojournTime}.
ask_r(Broker, ReqValue) ->
    call(Broker, bid, ReqValue, infinity).

%% @equiv nb_ask(Broker, self())
-spec nb_ask(Broker) -> Go | Retry when
      Broker :: broker(),
      Go :: {go, Ref, Value, RelativeTime, SojournTime},
      Ref :: reference(),
      Value :: any(),
      RelativeTime :: 0 | neg_integer(),
      SojournTime :: non_neg_integer(),
      Retry :: {retry, SojournTime}.
nb_ask(Broker) ->
    call(Broker, nb_ask, self(), infinity).

%% @doc Tries to match with a process calling `ask_r/2' on the same broker but
%% does not enqueue the request if no immediate match. Returns
%% `{go, Ref, Value, RelativeTime, SojournTime}' on a successful match or
%% `{retry, SojournTime}'.
%%
%% `Ref' is the transaction reference, which is a `reference()'. `Value' is the
%% value of the matched process. `RelativeTime' is the time spent waiting for a
%% match after discounting time spent waiting for the broker to handle requests.
%% `SojournTime' is the time spent in the broker's message queue.
%%
%% @see ask/2
-spec nb_ask(Broker, ReqValue) -> Go | Retry when
      Broker :: broker(),
      ReqValue :: any(),
      Go :: {go, Ref, Value, RelativeTime, SojournTime},
      Ref :: reference(),
      Value :: any(),
      RelativeTime :: 0 | neg_integer(),
      SojournTime :: non_neg_integer(),
      Retry :: {retry, SojournTime}.
nb_ask(Broker, ReqValue) ->
    call(Broker, nb_ask, ReqValue, infinity).

%% @equiv nb_ask_r(Broker, self())
-spec nb_ask_r(Broker) -> Go | Retry when
      Broker :: broker(),
      Go :: {go, Ref, Value, RelativeTime, SojournTime},
      Ref :: reference(),
      Value :: any(),
      RelativeTime :: 0 | neg_integer(),
      SojournTime :: non_neg_integer(),
      Retry :: {retry, SojournTime}.
nb_ask_r(Broker) ->
    call(Broker, nb_bid, self(), infinity).

%% @doc Tries to match with a process calling `ask/2' on the same broker but
%% does not enqueue the request if no immediate match.
%%
%% @see nb_ask/2
-spec nb_ask_r(Broker, ReqValue) -> Go | Retry when
      Broker :: broker(),
      ReqValue :: any(),
      Go :: {go, Ref, Value, RelativeTime, SojournTime},
      Ref :: reference(),
      Value :: any(),
      RelativeTime :: 0 | neg_integer(),
      SojournTime :: non_neg_integer(),
      Retry :: {retry, SojournTime}.
nb_ask_r(Broker, ReqValue) ->
    call(Broker, nb_bid, ReqValue, infinity).

%% @equiv async_ask(Broker, self())
-spec async_ask(Broker) -> {await, Tag, Pid} when
      Broker :: broker(),
      Tag :: reference(),
      Pid :: pid().
async_ask(Broker) ->
    async_call(Broker, ask, self()).

%% @doc Monitors the broker and sends an asynchronous request to match with a
%% process calling `ask_r/2'. Returns `{await, Tag, Pid}'.
%%
%% `Tag' is a monitor `reference()' that uniquely identifies the reply
%% containing the result of the request. `Pid', is the pid (`pid()') of the
%% monitored broker. To cancel the request call `cancel(Pid, Tag)'.
%%
%% The reply is of the form `{Tag, {go, Ref, Value, RelativeTime, SojournTime}'
%% or `{Tag, {drop, SojournTime}}'.
%%
%% `Ref' is the transaction reference, which is a `reference()'. `Value' is the
%% value of the matched process. `RelativeTime' is the time spent waiting for a
%% match after discounting time spent waiting for the broker to handle requests.
%% `SojournTime' is the time spent in the broker's message queue.
%%
%% Multiple asynchronous requests can be made from a single process to a
%% broker and no guarantee is made of the order of replies. A process making
%% multiple requests can reuse the monitor reference for subsequent requests to
%% the same broker process (`Process') using `async_ask/3'.
%%
%% @see cancel/2
%% @see async_ask/3
-spec async_ask(Broker, ReqValue) -> {await, Tag, Pid} when
      Broker :: broker(),
      ReqValue :: any(),
      Tag :: reference(),
      Pid :: pid().
async_ask(Broker, ReqValue) ->
    async_call(Broker, ask, ReqValue).

%% @doc Sends an asynchronous request to match with a process calling `ask_r/2'.
%% Returns `{await, Tag, Pid}'.
%%
%% `Tag' is a `any()' that identifies the reply containing the result of the
%% request. `Pid', is the pid (`pid()') of the broker. To cancel all requests
%% identified by `Tag' on broker `Pid' call `cancel(Pid, Tag)'.
%%
%% The reply is of the form `{Tag, {go, Ref, Value, RelativeTime, SojournTime}'
%% or `{Tag, {drop, SojournTime}}'.
%%
%% `Ref' is the transaction reference, which is a `reference()'. `Value' is the
%% value of the matched process. `RelativeTime' is the time spent waiting for a
%% match after discounting time spent waiting for the broker to handle requests.
%% `SojournTime' is the time spent in the broker's message queue.
%%
%% Multiple asynchronous requests can be made from a single process to a
%% broker and no guarantee is made of the order of replies. If the broker
%% exits or is on a disconnected node there is no guarantee of a reply and so
%% the caller should take appropriate steps to handle this scenario.
%%
%% @see cancel/2
-spec async_ask(Broker, ReqValue, Tag) -> {await, Tag, Pid} when
      Broker :: broker(),
      ReqValue :: any(),
      Tag :: any(),
      Pid :: pid().
async_ask(Broker, ReqValue, Tag) ->
    async_call(Broker, ask, ReqValue, Tag).

%% @equiv async_ask_r(Broker, self())
-spec async_ask_r(Broker) -> {await, Tag, Pid} when
      Broker :: broker(),
      Tag :: reference(),
      Pid :: pid().
async_ask_r(Broker) ->
    async_call(Broker, bid, self()).

%% @doc Monitors the broker and sends an asynchronous request to match with a
%% process calling `ask/2'.
%%
%% @see async_ask/2
%% @see cancel/2
-spec async_ask_r(Broker, ReqValue) -> {await, Tag, Pid} when
      Broker :: broker(),
      ReqValue :: any(),
      Tag :: reference(),
      Pid :: pid().
async_ask_r(Broker, ReqValue) ->
    async_call(Broker, bid, ReqValue).

%% @doc Sends an asynchronous request to match with a process calling `ask/2'.
%%
%% @see async_ask/3
%% @see cancel/2
-spec async_ask_r(Broker, ReqValue, Tag) -> {await, Tag, Pid} when
      Broker :: broker(),
      ReqValue :: any(),
      Tag :: any(),
      Pid :: pid().
async_ask_r(Broker, ReqValue, Tag) ->
    async_call(Broker, bid, ReqValue, Tag).

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
start_link(Mod, Args0, Opts) ->
    {Args1, GenOpts} = split_options(Args0, Opts),
    gen:start(?MODULE, link, Mod, Args1, GenOpts).

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
start_link(Name, Mod, Args0, Opts) ->
    {Args1, GenOpts} = split_options(Args0, Opts),
    gen:start(?MODULE, link, Name, Mod, Args1, GenOpts).

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
init_it(Starter, Parent, Name, Mod, {TimeOpts, Args}, Opts) ->
    DbgOpts = proplists:get_value(debug, Opts, []),
    Dbg = sys:debug_options(DbgOpts),
    {TimeMod, TimeUnit, ReadAfter} = TimeOpts,
    _ = put('$initial_call', {Mod, init, 1}),
    try Mod:init(Args) of
        {ok, {{AskMod, AskArgs}, {BidMod, BidArgs}}} ->
            Config = #config{mod=Mod, args=Args, parent=Parent, dbg=Dbg,
                             name=Name, time_mod=TimeMod, time_unit=TimeUnit,
                             ask_mod=AskMod, bid_mod=BidMod},
            Now = monotonic_time(Config),
            Time = #time{now=Now, send=Now, read_after=ReadAfter},
            init_asks(Starter, Time, AskArgs, BidArgs, Config);
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
system_continue(Parent, Dbg, [State, Time, Asks, Bids, Config]) ->
    NTime = read_time(Time, Config),
    NConfig = Config#config{parent=Parent, dbg=Dbg},
    timeout(State, NTime, Asks, Bids, NConfig);
system_continue(Parent, Dbg,
                {change, {AskMod, AskArgs, BidMod, BidArgs},
                [State, Time, Asks, Bids, Config]}) ->
    NTime = read_time(Time, Config),
    NConfig = Config#config{parent=Parent, dbg=Dbg},
    {NAsks, AskNext, NBids, BidNext, NConfig2} =
        change_asks(Time, Asks, AskMod, AskArgs, Bids, BidMod, BidArgs,
                    NConfig),
    next(State, NTime, NAsks, AskNext, NBids, BidNext, NConfig2).

%% @private
system_code_change([_, _, _, _, Config] = Misc, _, _, _) ->
    case config_change(Config) of
        {ok, Change} ->
            {ok, {change, Change, Misc}};
        ignore ->
            {ok, Misc};
        {error, Reason} ->
            % sys will turn this into {error, Reason}
            Reason
    end;
system_code_change({change, Change, [_, _, _, _, Config] = Misc}, _, _, _) ->
    case config_change(Config) of
        {ok, NChange} ->
            {ok, {change, NChange, Misc}};
        ignore ->
            {ok, {change, Change, Misc}};
        {error, Reason} ->
            % sys will turn this into {error, Reason}
            Reason
    end.

%% @private
system_get_state([_, _, Asks, Bids,
                  #config{ask_mod=AskMod, bid_mod=BidMod}]) ->
    Callbacks = [{AskMod, ask, Asks},
                 {BidMod, ask_r, Bids}],
    {ok, Callbacks};
system_get_state({change, _, Misc}) ->
    system_get_state(Misc).

%% @private
system_replace_state(Replace,
                     [State, Time, Asks, Bids,
                      #config{ask_mod=AskMod, bid_mod=BidMod} = Config]) ->
    {AskMod, ask, NAsks} = AskRes = Replace({AskMod, ask, Asks}),
    {BidMod, ask_r, NBids} = BidRes = Replace({BidMod, ask_r, Bids}),
    {ok, [AskRes, BidRes], [State, Time, NAsks, NBids, Config]};
system_replace_state(Replace, {change, Change, Misc}) ->
    {ok, States, NMisc} = system_replace_state(Replace, Misc),
    {ok, States, {change, Change, NMisc}}.

%% @private
system_terminate(Reason, Parent, Dbg, [_, _, Asks, Bids, Config]) ->
    NConfig = Config#config{parent=Parent, dbg=Dbg},
    terminate({stop, Reason}, Asks, Bids, NConfig).

%% @private
format_status(Opt,
              [PDict, SysState, Parent, _,
               [State, #time{now=Now}, Asks, Bids,
                #config{name=Name, ask_mod=AskMod, bid_mod=BidMod,
                        time_mod=TimeMod, time_unit=TimeUnit}]]) ->
    Header = gen:format_status_header("Status for sbroker", Name),
    Queues = [{AskMod, ask, Asks}, {BidMod, ask_r, Bids}],
    Queues2 = [{Mod, Id, format_queue(Mod, Opt, PDict, Queue)} ||
                {Mod, Id, Queue} <- Queues],
    [{header, Header},
     {data, [{"Status", SysState},
             {"Parent", Parent},
             {"Active queue", format_state(State)},
             {"Time", {TimeMod, TimeUnit, Now}}]},
     {items, {"Installed queues", Queues2}}];
format_status(Opt, [PDict, SysState, Parent, Dbg, {change, _, Misc}]) ->
    format_status(Opt, [PDict, SysState, Parent, Dbg, Misc]).

%% Internal

split_options(Args, Opts0) ->
    TimeOpts = time_options(Opts0),
    Opts1 = lists:keydelete(time_module, 1, Opts0),
    Opts2 = lists:keydelete(time_unit, 1, Opts1),
    Opts3 = lists:keydelete(read_time_after, 1, Opts2),
    {{TimeOpts, Args}, Opts3}.

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

init_asks(Starter, #time{now=Now} = Time, AskArgs, BidArgs,
          #config{ask_mod=AskMod} = Config) ->
    try AskMod:init(Now, AskArgs) of
        Asks ->
            init_bids(Starter, Time, Asks, BidArgs, Config)
    catch
        Class:Reason ->
            Stack = erlang:get_stacktrace(),
            init_exception(Starter, Class, Reason, Stack, [], Config)
    end.

init_bids(Starter, #time{now=Now} = Time, Asks, BidArgs,
          #config{bid_mod=BidMod, ask_mod=AskMod} = Config) ->
    try BidMod:init(Now, BidArgs) of
        Bids ->
            enter_loop(Starter, Time, Asks, Bids, Config)
    catch
        Class:Reason ->
            Stack = erlang:get_stacktrace(),
            Callbacks = [{AskMod, stop, Asks}],
            init_exception(Starter, Class, Reason, Stack, Callbacks, Config)
    end.

init_exception(Starter, Name, Class, Reason, Stack) ->
    Reason2 = sbroker_queue:exit_reason({Class, Reason, Stack}),
    init_stop(Starter, Name, {error, Reason2}, Reason2).

init_exception(Starter, Class, Reason, Stack, Callbacks,
               #config{name=Name} = Config) ->
    Reason2 = {Class, Reason, Stack},
    {stop, Reason3} = terminate(Reason2, Callbacks, Config),
    init_stop(Starter, Name, {error, Reason3}, Reason3).

enter_loop(Starter, Time, Asks, Bids, Config) ->
    proc_lib:init_ack(Starter, {ok, self()}),
    asking_idle(Time, Asks, Bids, infinity, Config).

read_time(Time, Config) ->
    Now = monotonic_time(Config),
    Time#time{now=Now, seq=0}.

monotonic_time(#config{time_mod=TimeMod, time_unit=native}) ->
    TimeMod:monotonic_time();
monotonic_time(#config{time_mod=TimeMod, time_unit=TimeUnit}) ->
    TimeMod:monotonic_time(TimeUnit).

mark(Time, Config) ->
    Now = monotonic_time(Config),
    _ = self() ! {'$mark', Now},
    Time#time{now=Now, send=Now, seq=0}.

handle_mark(Mark, #time{now=Now} = Time) ->
    _ = self() ! {'$mark', Now},
    Send = (Mark + Now) div 2,
    Time#time{send=Send}.

idle_timeout(_, infinity, _) ->
    infinity;
idle_timeout(#time{now=Now}, Next,
             #config{time_mod=TimeMod, time_unit=TimeUnit}) ->
    case TimeMod:convert_time_unit(Next-Now, TimeUnit, milli_seconds) of
        0       -> 1;
        Timeout -> Timeout
    end.

asking_idle(Time, Asks, Bids, Next, Config) ->
    Timeout = idle_timeout(Time, Next, Config),
    receive
        Msg ->
            NTime = mark(Time, Config),
            asking(Msg, NTime, Asks, Bids, infinity, Config)
    after
        Timeout ->
            NTime = mark(Time, Config),
            asking_timeout(NTime, Asks, Bids, Config)
    end.

asking_timeout(#time{now=Now} = Time, Asks, Bids,
               #config{ask_mod=AskMod} = Config) ->
    try AskMod:handle_timeout(Now, Asks) of
        {NAsks, Next} ->
            asking(Time, NAsks, Bids, Next, Config);
        Other ->
            asking_return(Other, Asks, Bids, Config)
    catch
        Class:Reason ->
            asking_exception(Class, Reason, Asks, Bids, Config)
    end.

asking(#time{seq=Seq, read_after=Seq} = Time, Asks, Bids, Next, Config) ->
    receive
        Msg ->
            #time{now=Now} = NTime = read_time(Time, Config),
            asking(Msg, NTime, Asks, Bids, max(Now, Next), Config)
    end;
asking(#time{seq=Seq} = Time, Asks, Bids, Next, Config) ->
    receive
        Msg ->
            asking(Msg, Time#time{seq=Seq+1}, Asks, Bids, Next, Config)
    end.

asking({ask, Ask, Value}, #time{now=Now, send=Send} = Time, Asks, Bids, _,
       #config{ask_mod=AskMod} = Config) ->
    try AskMod:handle_in(Send, Ask, Value, Now, Asks) of
        {NAsks, Next} ->
            asking(Time, NAsks, Bids, Next, Config);
        Other ->
            asking_return(Other, Asks, Bids, Config)
    catch
        Class:Reason ->
            asking_exception(Class, Reason, Asks, Bids, Config)
    end;
asking({bid, Bid, BidValue} = Msg, #time{now=Now} = Time, Asks, Bids, _,
       #config{ask_mod=AskMod} = Config) ->
    try AskMod:handle_out(Now, Asks) of
        {AskSend, Ask, AskValue, NAsks, Next} ->
            ask_settle(Time, AskSend, Ask, AskValue, Bid, BidValue),
            asking(Time, NAsks, Bids, Next, Config);
        {empty, NAsks} ->
            bidding(Msg, Time, NAsks, Bids, infinity, Config);
        Other ->
            asking_return(Other, Asks, Bids, Config)
    catch
        Class:Reason ->
            asking_exception(Class, Reason, Asks, Bids, Config)
    end;
asking({nb_ask, Ask, _}, Time, Asks, Bids, _, Config) ->
    retry(Ask, Time),
    asking_timeout(Time, Asks, Bids, Config);
asking({nb_bid, Bid, BidValue}, #time{now=Now} = Time, Asks, Bids, _,
       #config{ask_mod=AskMod} = Config) ->
    try AskMod:handle_out(Now, Asks) of
        {AskSend, Ask, AskValue, NAsks, Next} ->
            ask_settle(Time, AskSend, Ask, AskValue, Bid, BidValue),
            asking(Time, NAsks, Bids, Next, Config);
        {empty, NAsks} ->
            retry(Bid, Time),
            bidding(Time, NAsks, Bids, infinity, Config);
        Other ->
            asking_return(Other, Asks, Bids, Config)
    catch
        Class:Reason ->
            asking_exception(Class, Reason, Asks, Bids, Config)
    end;
asking({cancel, From, Tag}, #time{now=Now} = Time, Asks, Bids, _,
       #config{ask_mod=AskMod} = Config) ->
    try AskMod:handle_cancel(Tag, Now, Asks) of
        {Reply, NAsks, Next} ->
            gen:reply(From, Reply),
            asking(Time, NAsks, Bids, Next, Config);
        Other ->
            asking_return(Other, Asks, Bids, Config)
    catch
        Class:Reason ->
            asking_exception(Class, Reason, Asks, Bids, Config)
    end;
asking({'$mark', Mark}, Time, Asks, Bids, Next, Config) ->
    receive
        Msg ->
            NTime = handle_mark(Mark, Time),
            asking(Msg, NTime, Asks, Bids, Next, Config)
    after
        0 ->
            asking_idle(Time, Asks, Bids, Next, Config)
    end;
asking({'EXIT', Parent, Reason}, _, Asks, Bids, _,
       #config{parent=Parent} = Config) ->
    terminate({stop, Reason}, Asks, Bids, Config);
asking({system, From, Msg}, Time, Asks, Bids, _, Config) ->
    system(From, Msg, asking, Time, Asks, Bids, Config);
asking({change_config, From, _}, Time, Asks, Bids, _, Config) ->
    config_change(From, asking, Time, Asks, Bids, Config);
asking({len_ask, From, _}, Time, Asks, Bids, _,
       #config{ask_mod=AskMod} = Config) ->
    try AskMod:len(Asks) of
        Len ->
            gen:reply(From, Len),
            asking_timeout(Time, Asks, Bids, Config)
    catch
        Class:Reason ->
            asking_exception(Class, Reason, Asks, Bids, Config)
    end;
asking({len_bid, From, _}, Time, Asks, Bids, _, Config) ->
    gen:reply(From, 0),
    asking_timeout(Time, Asks, Bids, Config);
asking(timeout, Time, Asks, Bids, _, Config) ->
    asking_timeout(Time, Asks, Bids, Config);
asking(Msg, Time, Asks, Bids, _, Config) ->
    info_asks(Msg, asking, Time, Asks, Bids, Config).

ask_settle(#time{now=Now, send=Send}, AskSend, Ask, AskValue, Bid, BidValue) ->
    settle(Now, AskSend, Ask, AskValue, Send, Bid, BidValue).

info_asks(Msg, State, #time{now=Now} = Time, Asks, Bids,
          #config{ask_mod=AskMod} = Config) ->
    try AskMod:handle_info(Msg, Now, Asks) of
        {NAsks, AskNext} ->
            info_bids(Msg, State, Time, NAsks, Bids, AskNext, Config);
        Other ->
            asking_return(Other, Asks, Bids, Config)
    catch
        Class:Reason ->
            asking_exception(Class, Reason, Asks, Bids, Config)
    end.

info_bids(Msg, State, #time{now=Now} = Time, Asks, Bids, AskNext,
          #config{bid_mod=BidMod} = Config) ->
    try BidMod:handle_info(Msg, Now, Bids) of
        {NBids, _} when State =:= asking ->
            asking(Time, Asks, NBids, AskNext, Config);
        {NBids, BidNext} when State =:= bidding ->
            bidding(Time, Asks, NBids, BidNext, Config);
        Other ->
            bidding_return(Other, Asks, Bids, Config)
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

bidding_idle(Time, Asks, Bids, Next, Config) ->
    Timeout = idle_timeout(Time, Next, Config),
    receive
        Msg ->
            NTime = mark(Time, Config),
            bidding(Msg, NTime, Asks, Bids, infinity, Config)
    after
        Timeout ->
            NTime = mark(Time, Config),
            bidding_timeout(NTime, Asks, Bids, Config)
    end.

bidding_timeout(#time{now=Now} = Time, Asks, Bids,
                #config{bid_mod=BidMod} = Config) ->
    try BidMod:handle_timeout(Now, Bids) of
        {NBids, Next} ->
            bidding(Time, Asks, NBids, Next, Config);
        Other ->
            bidding_return(Other, Asks, Bids, Config)
    catch
        Class:Reason ->
            bidding_exception(Class, Reason, Asks, Bids, Config)
    end.

bidding(#time{seq=Seq, read_after=Seq} = Time, Asks, Bids, Next, Config) ->
    receive
        Msg ->
            #time{now=Now} = NTime = read_time(Time, Config),
            bidding(Msg, NTime, Asks, Bids, max(Now, Next), Config)
    end;
bidding(Time, Asks, Bids, Next, Config) ->
    receive
        Msg ->
            bidding(Msg, Time, Asks, Bids, Next, Config)
    end.

bidding({bid, Bid, Value}, #time{now=Now, send=Send} = Time, Asks, Bids, _,
        #config{bid_mod=BidMod} = Config) ->
    try BidMod:handle_in(Send, Bid, Value, Now, Bids) of
        {NBids, Next} ->
            bidding(Time, Asks, NBids, Next, Config);
        Other ->
            bidding_return(Other, Asks, Bids, Config)
    catch
        Class:Reason ->
            bidding_exception(Class, Reason, Asks, Bids, Config)
    end;
bidding({ask, Ask, AskValue} = Msg, #time{now=Now} = Time, Asks, Bids, _,
        #config{bid_mod=BidMod} = Config) ->
    try BidMod:handle_out(Now, Bids) of
        {BidSend, Bid, BidValue, NBids, Next} ->
            bid_settle(Time, Ask, AskValue, BidSend, Bid, BidValue),
            bidding(Time, Asks, NBids, Next, Config);
        {empty, NBids} ->
            asking(Msg, Time, Asks, NBids, infinity, Config);
        Other ->
            bidding_return(Other, Asks, Bids, Config)
    catch
        Class:Reason ->
            bidding_exception(Class, Reason, Asks, Bids, Config)
    end;
bidding({nb_bid, Bid, _}, Time, Asks, Bids, _, Config) ->
    retry(Bid, Time),
    bidding_timeout(Time, Asks, Bids, Config);
bidding({nb_ask, Ask, AskValue}, #time{now=Now} = Time, Asks, Bids, _,
        #config{bid_mod=BidMod} = Config) ->
    try BidMod:handle_out(Now, Bids) of
        {BidSend, Bid, BidValue, NBids, Next} ->
            bid_settle(Time, Ask, AskValue, BidSend, Bid, BidValue),
            bidding(Time, Asks, NBids, Next, Config);
        {empty, NBids} ->
            retry(Ask, Time),
            asking(Time, Asks, NBids, infinity, Config);
        Other ->
            bidding_return(Other, Asks, Bids, Config)
    catch
        Class:Reason ->
            bidding_exception(Class, Reason, Asks, Bids, Config)
    end;
bidding({cancel, From, Tag}, #time{now=Now} = Time, Asks, Bids, _,
        #config{bid_mod=BidMod} = Config) ->
    try BidMod:handle_cancel(Tag, Now, Bids) of
        {Reply, NBids, Next} ->
            gen:reply(From, Reply),
            bidding(Time, Asks, NBids, Next, Config);
        Other ->
            bidding_return(Other, Asks, Bids, Config)
    catch
        Class:Reason ->
            bidding_exception(Class, Reason, Asks, Bids, Config)
    end;
bidding({'$mark', Mark}, Time, Asks, Bids, Next, Config) ->
    receive
        Msg ->
            NTime = handle_mark(Mark, Time),
            bidding(Msg, NTime, Asks, Bids, Next, Config)
    after
        0 ->
            bidding_idle(Time, Asks, Bids, Next, Config)
    end;
bidding({'EXIT', Parent, Reason}, _, Asks, Bids, _,
        #config{parent=Parent} = Config) ->
    terminate({stop, Reason}, Asks, Bids, Config);
bidding({system, From, Msg}, Time, Asks, Bids, _, Config) ->
    system(From, Msg, bidding, Time, Asks, Bids, Config);
bidding({change_config, From, _}, Time, Asks, Bids, _, Config) ->
    config_change(From, bidding, Time, Asks, Bids, Config);
bidding({len_ask, From, _}, Time, Asks, Bids, _, Config) ->
    gen:reply(From, 0),
    bidding_timeout(Time, Asks, Bids, Config);
bidding({len_bid, From, _}, Time, Asks, Bids, _,
        #config{bid_mod=BidMod} = Config) ->
    try BidMod:len(Bids) of
        Len ->
            gen:reply(From, Len),
            bidding_timeout(Time, Asks, Bids, Config)
    catch
        Class:Reason ->
            bidding_exception(Class, Reason, Asks, Bids, Config)
    end;
bidding(timeout, Time, Asks, Bids, _, Config) ->
    bidding_timeout(Time, Asks, Bids, Config);
bidding(Msg, Time, Asks, Bids, _, Config) ->
    info_asks(Msg, bidding, Time, Asks, Bids, Config).

bid_settle(#time{now=Now, send=Send}, Ask, AskValue, BidSend, Bid, BidValue) ->
    settle(Now, Send, Ask, AskValue, BidSend, Bid, BidValue).

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

settle(Now, AskSend, Ask, AskValue, BidSend, Bid, BidValue) ->
    Ref = make_ref(),
    RelativeTime = AskSend - BidSend,
    %% Bid always messaged first.
    gen:reply(Bid, {go, Ref, AskValue, RelativeTime, Now - BidSend}),
    gen:reply(Ask, {go, Ref, BidValue, -RelativeTime, Now - AskSend}).

retry(From, #time{now=Now, send=Send}) ->
    gen:reply(From, {retry, Now - Send}).

config_change(From, State, Time, Asks, Bids, Config) ->
    case config_change(Config) of
        {ok, {AskMod, AskArgs, BidMod, BidArgs}} ->
            gen:reply(From, ok),
            {NAsks, AskNext, NBids, BidNext, NConfig} =
                change_asks(Time, Asks, AskMod, AskArgs, Bids, BidMod, BidArgs,
                            Config),
            next(State, Time, NAsks, AskNext, NBids, BidNext, NConfig);
        ignore  ->
            gen:reply(From, ok),
            timeout(State, Time, Asks, Bids, Config);
        {error, Reason} ->
            gen:reply(From, {error, Reason}),
            timeout(State, Time, Asks, Bids, Config)
    end.

config_change(#config{mod=Mod, args=Args}) ->
    try Mod:init(Args) of
        {ok, {{AskMod, AskArgs}, {BidMod, BidArgs}}} ->
            {ok, {AskMod, AskArgs, BidMod, BidArgs}};
        ignore ->
            ignore;
        Other ->
            {error, {bad_return_value, Other}}
    catch
        Class:Reason ->
            {error, {Class, Reason, erlang:get_stacktrace()}}
    end.

change_asks(Time, Asks, NAskMod, AskArgs, Bids, NBidMod, BidArgs,
            #config{ask_mod=AskMod, bid_mod=BidMod} = Config) ->
    case change(Time, AskMod, Asks, NAskMod, AskArgs, Config) of
        {ok, NAsks, AskNext} ->
            NConfig = Config#config{ask_mod=NAskMod},
            change_bids(Time, NAsks, AskNext, Bids, NBidMod, BidArgs, NConfig);
        {stop, _} = Stop ->
            terminate(Stop, [{BidMod, stop, Bids}], Config)
    end.

change_bids(Time, Asks, AskNext, Bids, NBidMod, BidArgs,
            #config{bid_mod=BidMod, ask_mod=AskMod} = Config) ->
    case change(Time, BidMod, Bids, NBidMod, BidArgs, Config) of
        {ok, NBids, BidNext} ->
            {Asks, AskNext, NBids, BidNext, Config#config{bid_mod=NBidMod}};
        {stop, _} = Stop ->
            terminate(Stop, [{AskMod, stop, Asks}], Config)
    end.

change(#time{now=Now}, Mod1, State1, Mod2, Args2, Config) ->
    sbroker_queue:change(Mod1, State1, Mod2, Args2, Now, report_name(Config)).

next(asking, Time, Asks, AskNext, Bids, _, Config) ->
    asking(Time, Asks, Bids, AskNext, Config);
next(bidding, Time, Asks, _, Bids, BidNext, Config) ->
    bidding(Time, Asks, Bids, BidNext, Config).

timeout(asking, Time, Asks, Bids, Config) ->
    asking_timeout(Time, Asks, Bids, Config);
timeout(bidding, Time, Asks, Bids, Config) ->
    bidding_timeout(Time, Asks, Bids, Config).

system(From, Msg, State, Time, Asks, Bids,
       #config{parent=Parent, dbg=Dbg} = Config) ->
    NConfig = Config#config{dbg=[]},
    sys:handle_system_msg(Msg, From, Parent, ?MODULE, Dbg,
                          [State, Time, Asks, Bids, NConfig]).

format_queue(Mod, Opt, PDict, State) ->
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

format_state(asking) ->
    ask;
format_state(bidding) ->
    ask_r.

terminate(Reason, Callbacks, Config) ->
    Name = report_name(Config),
    {stop, NReason} = sbroker_queue:terminate(Reason, Callbacks, Name),
    exit(NReason).

terminate(Reason, Asks, Bids,
          #config{ask_mod=AskMod, bid_mod=BidMod} = Config) ->
    terminate(Reason, [{AskMod, stop, Asks}, {BidMod, stop, Bids}], Config).

report_name(#config{name=Pid, mod=Mod}) when is_pid(Pid) ->
    {Mod, Pid};
report_name(#config{name=Name}) ->
    Name.
