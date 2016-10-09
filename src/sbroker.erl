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
%% two queues and is matched with a process in the other queue. The queues are
%% managed using `sbroker_queue' callback module per queue so that a different
%% strategy can be used for both queues. Processes that die while in a queue are
%% automatically removed to prevent matching with a process that is nolonger
%% alive. A broker also uses an `sbroker_meter' callback module to monitor the
%% queue and processing delays of the broker.
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
%% `{ok, {AskQueueSpec, AskRQueueSpec, [MeterSpec]})' or `ignore'.
%% `AskQueueSpec' is the queue specification for the `ask' queue,
%% `AskRQueueSpec' is the queue specification for the `ask_r' queue and
%% `MeterSpec' is a meter specification. There can any number of meters but a
%% meter module can only be included once. In the case of `ignore' the broker is
%% not started and `start_link' returns `ignore'. As the callback modules are
%% defined in the `init/1' callback a broker supports the `dynamic' modules
%% supervisor child specification.
%%
%% Both queue and meter specifcations take the form: `{Module, Args}'. `Module'
%% is the callback module and `Args' are its arguments.
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
%%     sbroker:start_link({local, ?MODULE}, ?MODULE, [], []).
%%
%% ask() ->
%%     sbroker:ask(?MODULE).
%%
%% ask_r() ->
%%     sbroker:ask_r(?MODULE).
%%
%% init([]) ->
%%     AskQueueSpec = {sbroker_codel_queue, #{}},
%%     AskRQueueSpec = {sbroker_timeout_queue, #{}},
%%     MeterSpec = {sbroker_overload_meter, #{alarm => {overload, ?MODULE}}},
%%     {ok, {AskQueueSpec, AskRQueueSpec, [MeterSpec]}}.
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
-export([dynamic_ask/1]).
-export([dynamic_ask/2]).
-export([dynamic_ask_r/1]).
-export([dynamic_ask_r/2]).
-export([await/2]).
-export([cancel/2]).
-export([cancel/3]).
-export([dirty_cancel/2]).
-export([change_config/1]).
-export([change_config/2]).
-export([len/1]).
-export([len/2]).
-export([len_r/1]).
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

%% types

-type broker() :: pid() | atom() | {atom(), node()} | {global, any()}
    | {via, module(), any()}.
-type name() :: {local, atom()} | {global, any()} | {via, module(), any()}.
-type debug_option() ::
    trace | log | {log, pos_integer()} | statistics |
    {log_to_file, file:filename()} | {install, {fun(), any()}}.
-type start_option() ::
    {debug, debug_option()} | {timeout, timeout()} |
    {spawn_opt, [proc_lib:spawn_option()]} |
    {read_time_after, non_neg_integer() | infinity}.
-type start_return() :: {ok, pid()} | ignore | {error, any()}.
-type handler_spec() :: {module(), any()}.

-export_type([broker/0]).
-export_type([handler_spec/0]).

-callback init(Args :: any()) ->
    {ok, {AskQueueSpec :: handler_spec(), AskRQueueSpec :: handler_spec(),
          [MeterSpec :: handler_spec()]}} | ignore.

-record(config, {mod :: module(),
                 args :: any(),
                 parent :: pid(),
                 dbg :: [sys:dbg_opt()],
                 name :: name() | pid(),
                 ask_mod :: module(),
                 bid_mod :: module()}).

-record(time, {now :: integer(),
               send :: integer(),
               empty :: integer(),
               next = infinity :: integer() | infinity,
               seq :: non_neg_integer(),
               read_after :: non_neg_integer() | infinity,
               meters :: [{module(), any()}]}).

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
    sbroker_gen:call(Broker, ask, self(), infinity).

%% @doc Send a match request, with value `ReqValue', to try to match with a
%% process calling `ask_r/2' on the broker, `Broker'.
%%
%% Returns `{go, Ref, Value, RelativeTime, SojournTime}' on a successful
%% match or `{drop, SojournTime}'.
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
%%
%% If the request is dropped when using `via' module `sprotector' returns
%% `{drop, 0}' and does not send the request.
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
    sbroker_gen:call(Broker, ask, ReqValue, infinity).

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
    sbroker_gen:call(Broker, bid, self(), infinity).

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
    sbroker_gen:call(Broker, bid, ReqValue, infinity).

%% @equiv nb_ask(Broker, self())
-spec nb_ask(Broker) -> Go | Drop when
      Broker :: broker(),
      Go :: {go, Ref, Value, RelativeTime, SojournTime},
      Ref :: reference(),
      Value :: any(),
      RelativeTime :: 0 | neg_integer(),
      SojournTime :: non_neg_integer(),
      Drop :: {drop, SojournTime}.
nb_ask(Broker) ->
    sbroker_gen:call(Broker, nb_ask, self(), infinity).

%% @doc Tries to match with a process calling `ask_r/2' on the same broker but
%% does not enqueue the request if no immediate match. Returns
%% `{go, Ref, Value, RelativeTime, SojournTime}' on a successful match or
%% `{drop, SojournTime}'.
%%
%% `Ref' is the transaction reference, which is a `reference()'. `Value' is the
%% value of the matched process. `RelativeTime' is the time spent waiting for a
%% match after discounting time spent waiting for the broker to handle requests.
%% `SojournTime' is the time spent in the broker's message queue.
%%
%% If the request is dropped when using `via' module `sprotector' returns
%% `{drop, 0}' and does not send the request.
%%
%% @see ask/2
-spec nb_ask(Broker, ReqValue) -> Go | Drop when
      Broker :: broker(),
      ReqValue :: any(),
      Go :: {go, Ref, Value, RelativeTime, SojournTime},
      Ref :: reference(),
      Value :: any(),
      RelativeTime :: 0 | neg_integer(),
      SojournTime :: non_neg_integer(),
      Drop :: {drop, SojournTime}.
nb_ask(Broker, ReqValue) ->
    sbroker_gen:call(Broker, nb_ask, ReqValue, infinity).

%% @equiv nb_ask_r(Broker, self())
-spec nb_ask_r(Broker) -> Go | Drop when
      Broker :: broker(),
      Go :: {go, Ref, Value, RelativeTime, SojournTime},
      Ref :: reference(),
      Value :: any(),
      RelativeTime :: 0 | neg_integer(),
      SojournTime :: non_neg_integer(),
      Drop :: {drop, SojournTime}.
nb_ask_r(Broker) ->
    sbroker_gen:call(Broker, nb_bid, self(), infinity).

%% @doc Tries to match with a process calling `ask/2' on the same broker but
%% does not enqueue the request if no immediate match.
%%
%% @see nb_ask/2
-spec nb_ask_r(Broker, ReqValue) -> Go | Drop when
      Broker :: broker(),
      ReqValue :: any(),
      Go :: {go, Ref, Value, RelativeTime, SojournTime},
      Ref :: reference(),
      Value :: any(),
      RelativeTime :: 0 | neg_integer(),
      SojournTime :: non_neg_integer(),
      Drop :: {drop, SojournTime}.
nb_ask_r(Broker, ReqValue) ->
    sbroker_gen:call(Broker, nb_bid, ReqValue, infinity).

%% @equiv async_ask(Broker, self())
-spec async_ask(Broker) -> {await, Tag, Process} | {drop, 0} when
      Broker :: broker(),
      Tag :: reference(),
      Process :: pid() | {atom(), node()}.
async_ask(Broker) ->
    sbroker_gen:async_call(Broker, ask, self()).

%% @doc Monitors the broker and sends an asynchronous request to match with a
%% process calling `ask_r/2'. Returns `{await, Tag, Pid}' or `{drop, 0}'.
%%
%% `Tag' is a monitor `reference()' that uniquely identifies the reply
%% containing the result of the request. `Process', is the `pid()' of the
%% monitored broker or `{atom(), node()}' if the broker is registered locally
%% in another node. To cancel the request call `cancel(Process, Tag)'.
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
%% If the request is dropped when using `via' module `sprotector' returns
%% `{drop, 0}' and does not send the request.
%%
%% @see cancel/2
%% @see async_ask/3
-spec async_ask(Broker, ReqValue) -> {await, Tag, Process} | {drop, 0} when
      Broker :: broker(),
      ReqValue :: any(),
      Tag :: reference(),
      Process :: pid() | {atom(), node()}.
async_ask(Broker, ReqValue) ->
    sbroker_gen:async_call(Broker, ask, ReqValue).

%% @doc Sends an asynchronous request to match with a process calling `ask_r/2'.
%% Returns `{await, Tag, Pid}'.
%%
%% `To' is a tuple containing the process, `pid()', to send the reply to and
%% `Tag', `any()', that identifies the reply containing the result of the
%% request. `Process' is the `pid()' of the broker or `{atom(), node()}' if the
%% broker is registered locally on a different node. To cancel all requests
%% identified by `Tag' on broker `Process' call `cancel(Process, Tag)'.
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
%% If the request is dropped when using `via' module `sprotector', returns
%% `{drop, 0}' and does not send the request.
%%
%% @see cancel/2
-spec async_ask(Broker, ReqValue, To) -> {await, Tag, Process} | {drop, 0} when
      Broker :: broker(),
      ReqValue :: any(),
      To :: {Pid, Tag},
      Pid :: pid(),
      Tag :: any(),
      Process :: pid() | {atom(), node()}.
async_ask(Broker, ReqValue, To) ->
    sbroker_gen:async_call(Broker, ask, ReqValue, To).

%% @equiv async_ask_r(Broker, self())
-spec async_ask_r(Broker) -> {await, Tag, Process} | {drop, 0} when
      Broker :: broker(),
      Tag :: reference(),
      Process :: pid() | {atom(), node()}.
async_ask_r(Broker) ->
    sbroker_gen:async_call(Broker, bid, self()).

%% @doc Monitors the broker and sends an asynchronous request to match with a
%% process calling `ask/2'.
%%
%% @see async_ask/2
%% @see cancel/2
-spec async_ask_r(Broker, ReqValue) -> {await, Tag, Process} | {drop, 0} when
      Broker :: broker(),
      ReqValue :: any(),
      Tag :: reference(),
      Process :: pid() | {atom(), node()}.
async_ask_r(Broker, ReqValue) ->
    sbroker_gen:async_call(Broker, bid, ReqValue).

%% @doc Sends an asynchronous request to match with a process calling `ask/2'.
%%
%% @see async_ask/3
%% @see cancel/2
-spec async_ask_r(Broker, ReqValue, To) ->
    {await, Tag, Process} | {drop, 0} when
      Broker :: broker(),
      ReqValue :: any(),
      To :: {Pid, Tag},
      Pid :: pid(),
      Tag :: any(),
      Process :: pid() | {atom(), node()}.
async_ask_r(Broker, ReqValue, To) ->
    sbroker_gen:async_call(Broker, bid, ReqValue, To).

%% @equiv dynamic_ask(Broker, self())
-spec dynamic_ask(Broker) -> Go | Await | Drop when
      Broker :: broker(),
      Go :: {go, Ref, Value, RelativeTime, SojournTime},
      Ref :: reference(),
      Value :: any(),
      RelativeTime :: 0 | neg_integer(),
      SojournTime :: non_neg_integer(),
      Await :: {await, Tag, Pid},
      Tag :: reference(),
      Pid :: pid(),
      Drop :: {drop, SojournTime}.
dynamic_ask(Broker) ->
    sbroker_gen:dynamic_call(Broker, dynamic_ask, self(), infinity).

%% @doc Tries to match with a process calling `ask_r/2' on the same broker. If
%% no immediate match available the request is converted to an `async_ask/2'.
%%
%% Returns `{go, Ref, Value, RelativeTime, SojournTime}' on a successful match
%% or `{await, Tag, BrokerPid}'.
%%
%% `Ref' is the transaction reference, which is a `reference()'. `Value' is the
%% value of the matched process. `RelativeTime' is the time spent waiting for a
%% match after discounting time spent waiting for the broker to handle requests.
%% `SojournTime' is the time spent in the broker's message queue. `Tag' is a
%% monitor reference and `BrokerPid' the `pid()' of the broker, as returned by
%% `async_ask/2'.
%%
%% If the request is dropped when using `via' module `sprotector' returns
%% `{drop, 0}' and does not send the request.
%%
%% @see nb_ask/2
%% @see async_ask/2
-spec dynamic_ask(Broker, ReqValue) -> Go | Await | Drop when
      Broker :: broker(),
      ReqValue :: any(),
      Go :: {go, Ref, Value, RelativeTime, SojournTime},
      Ref :: reference(),
      Value :: any(),
      RelativeTime :: 0 | neg_integer(),
      SojournTime :: non_neg_integer(),
      Await :: {await, Tag, Pid},
      Tag :: reference(),
      Pid :: pid(),
      Drop :: {drop, SojournTime}.
dynamic_ask(Broker, ReqValue) ->
    sbroker_gen:dynamic_call(Broker, dynamic_ask, ReqValue, infinity).

%% @equiv dynamic_ask_r(Broker, self())
-spec dynamic_ask_r(Broker) -> Go | Await | Drop when
      Broker :: broker(),
      Go :: {go, Ref, Value, RelativeTime, SojournTime},
      Ref :: reference(),
      Value :: any(),
      RelativeTime :: 0 | neg_integer(),
      SojournTime :: non_neg_integer(),
      Await :: {await, Tag, Pid},
      Tag :: reference(),
      Pid :: pid(),
      Drop :: {drop, SojournTime}.
dynamic_ask_r(Broker) ->
    sbroker_gen:dynamic_call(Broker, dynamic_bid, self(), infinity).

%% @doc Tries to match with a process calling `ask/2' on the same broker. If
%% no immediate match available the request is converted to an `async_ask_r/2'.
%%
%% @see dynamic_ask/2
-spec dynamic_ask_r(Broker, ReqValue) -> Go | Await | Drop when
      Broker :: broker(),
      ReqValue :: any(),
      Go :: {go, Ref, Value, RelativeTime, SojournTime},
      Ref :: reference(),
      Value :: any(),
      RelativeTime :: 0 | neg_integer(),
      SojournTime :: non_neg_integer(),
      Await :: {await, Tag, Pid},
      Tag :: reference(),
      Pid :: pid(),
      Drop :: {drop, SojournTime}.
dynamic_ask_r(Broker, ReqValue) ->
    sbroker_gen:dynamic_call(Broker, dynamic_bid, ReqValue, infinity).

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

%% @equiv cancel(Broker, Tag, infinity)
-spec cancel(Broker, Tag) -> Count | false when
      Broker :: broker(),
      Tag :: any(),
      Count :: pos_integer().
cancel(Broker, Tag) ->
    cancel(Broker, Tag, infinity).

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
    sbroker_gen:simple_call(Broker, cancel, Tag, Timeout).

%% @doc Cancels an asynchronous request.
%%
%% Returns `ok' without waiting for the broker to cancel requests.
%%
%% @see cancel/3
-spec dirty_cancel(Broker, Tag) -> ok when
      Broker :: broker(),
      Tag :: any().
dirty_cancel(Broker, Tag) ->
    sbroker_gen:send(Broker, {cancel, dirty, Tag}).

%% @equiv change_config(Broker, infinity)
-spec change_config(Broker) -> ok | {error, Reason} when
      Broker :: broker(),
      Reason :: any().
change_config(Broker) ->
    change_config(Broker, infinity).

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
    sbroker_gen:simple_call(Broker, change_config, undefined, Timeout).

%% @equiv len(Broker, infinity)
-spec len(Broker) -> Length when
      Broker :: broker(),
      Length :: non_neg_integer().
len(Broker) ->
    len(Broker, infinity).

%% @doc Get the length of the `ask' queue in the broker, `Broker'.
-spec len(Broker, Timeout) -> Length when
      Broker :: broker(),
      Timeout :: timeout(),
      Length :: non_neg_integer().
len(Broker, Timeout) ->
    sbroker_gen:simple_call(Broker, len_ask, undefined, Timeout).

%% @equiv len_r(Broker, infinity)
-spec len_r(Broker) -> Length when
      Broker :: broker(),
      Length :: non_neg_integer().
len_r(Broker) ->
    len_r(Broker, infinity).

%% @doc Get the length of the `ask_r' queue in the broker, `Broker'.
-spec len_r(Broker, Timeout) -> Length when
      Broker :: broker(),
      Timeout :: timeout(),
      Length :: non_neg_integer().
len_r(Broker, Timeout) ->
    sbroker_gen:simple_call(Broker, len_bid, undefined, Timeout).

%% @doc Starts a broker with callback module `Module' and argument `Args', and
%% broker options `Opts'.
%%
%% `Opts' is a `proplist' and supports `debug', `timeout' and `spawn_opt' used
%% by `gen_server' and `gen_fsm'. `read_time_after' sets the number of requests
%% when a cached time is stale and the time is read again. Its value is
%% `non_neg_integer()' or `infinity' and defaults to `16'.
%%
%% @see gen_server:start_link/3
-spec start_link(Module, Args, Opts) -> StartReturn when
      Module :: module(),
      Args :: any(),
      Opts :: [start_option()],
      StartReturn :: start_return().
start_link(Mod, Args, Opts) ->
    sbroker_gen:start_link(?MODULE, Mod, Args, Opts).

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
    sbroker_gen:start_link(Name, ?MODULE, Mod, Args, Opts).

%% test api

%% @hidden
-spec timeout(Broker) -> ok when
      Broker :: broker().
timeout(Broker) ->
    sbroker_gen:send(Broker, timeout).

%% gen api

%% Inside the broker an ask_r request is refered to as a bid to make the
%% difference between ask and ask_r clearer.

%% @private
init_it(Starter, Parent, Name, Mod, Args, Opts) ->
    DbgOpts = proplists:get_value(debug, Opts, []),
    Dbg = sys:debug_options(DbgOpts),
    ReadAfter = proplists:get_value(read_time_after, Opts),
    try Mod:init(Args) of
        {ok, {{AskMod, AskArgs}, {BidMod, BidArgs}, MeterArgs}}
          when is_list(MeterArgs) ->
            Config = #config{mod=Mod, args=Args, parent=Parent, dbg=Dbg,
                             name=Name, ask_mod=AskMod, bid_mod=BidMod},
            Now = erlang:monotonic_time(),
            Time = #time{now=Now, send=Now, empty=Now, read_after=ReadAfter,
                         seq=0, meters=[]},
            init(Starter, Time, AskArgs, BidArgs, MeterArgs, Config);
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
system_continue(Parent, Dbg, [State, Time, Asks, Bids, Config]) ->
    NConfig = Config#config{parent=Parent, dbg=Dbg},
    timeout(State, Time, Asks, Bids, NConfig);
system_continue(Parent, Dbg,
                {change, Change, [State, Time, Asks, Bids, Config]}) ->
    NConfig = Config#config{parent=Parent, dbg=Dbg},
    change(State, Change, Time, Asks, Bids, NConfig).

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
                    [_, _, _, _, #config{mod=Mod} = Config] = Misc}, Mod, _,
                   _) ->
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
system_get_state([_, #time{meters=Meters}, Asks, Bids,
                  #config{ask_mod=AskMod, bid_mod=BidMod}]) ->
    Meters2 = [{MeterMod, meter, Meter} || {MeterMod, Meter} <- Meters],
    Callbacks = [{AskMod, ask, Asks}, {BidMod, ask_r, Bids} | Meters2],
    {ok, Callbacks};
system_get_state({change, _, Misc}) ->
    system_get_state(Misc).

%% @private
system_replace_state(Replace,
                     [State, #time{meters=Meters} = Time, Asks, Bids,
                      #config{ask_mod=AskMod, bid_mod=BidMod} = Config]) ->
    {AskMod, ask, NAsks} = AskRes = Replace({AskMod, ask, Asks}),
    {BidMod, ask_r, NBids} = BidRes = Replace({BidMod, ask_r, Bids}),
    MetersRes = [{MeterMod, meter, _} = Replace({MeterMod, meter, Meter}) ||
                 {MeterMod, Meter} <- Meters],
    Result = [AskRes, BidRes, MetersRes],
    NMeters = [{MeterMod, NMeter} || {MeterMod, meter, NMeter} <- MetersRes],
    Misc = [State, Time#time{meters=NMeters}, NAsks, NBids, Config],
    {ok, Result, Misc};
system_replace_state(Replace, {change, Change, Misc}) ->
    {ok, States, NMisc} = system_replace_state(Replace, Misc),
    {ok, States, {change, Change, NMisc}}.

%% @private
system_terminate(Reason, Parent, Dbg, [_, Time, Asks, Bids, Config]) ->
    NConfig = Config#config{parent=Parent, dbg=Dbg},
    terminate({stop, Reason}, Time, Asks, Bids, NConfig);
system_terminate(Reason, Parent, Dbg, {change, _, Misc}) ->
    system_terminate(Reason, Parent, Dbg, Misc).

%% @private
format_status(Opt,
              [PDict, SysState, Parent, _,
               [State, #time{now=Now, meters=Meters}, Asks, Bids,
                #config{name=Name, ask_mod=AskMod, bid_mod=BidMod}]]) ->
    Header = gen:format_status_header("Status for sbroker", Name),
    Meters2 = [{MeterMod, meter, Meter} || {MeterMod, Meter} <- Meters],
    Handlers = [{AskMod, ask, Asks}, {BidMod, ask_r, Bids} | Meters2],
    Handlers2 = [{Mod, Id, format_status(Mod, Opt, PDict, Handler)} ||
                {Mod, Id, Handler} <- Handlers],
    [{header, Header},
     {data, [{"Status", SysState},
             {"Parent", Parent},
             {"Active queue", format_state(State)},
             {"Time", Now}]},
     {items, {"Installed handlers", Handlers2}}];
format_status(Opt, [PDict, SysState, Parent, Dbg, {change, _, Misc}]) ->
    format_status(Opt, [PDict, SysState, Parent, Dbg, Misc]).

%% Internal

init(Starter, Time, AskArgs, BidArgs, MeterArgs,
     #config{ask_mod=AskMod, bid_mod=BidMod, name=Name} = Config) ->
    case check_meters(MeterArgs) of
        ok ->
            do_init(Starter, Time, AskArgs, BidArgs, MeterArgs, Config);
        {error, Reason} ->
            Return = {ok, {{AskMod, AskArgs}, {BidMod, BidArgs}, MeterArgs}},
            init_stop(Starter, Name, {Reason, Return})
    end.

do_init(Starter, #time{now=Now, send=Send} = Time, AskArgs, BidArgs, MeterArgs,
     #config{ask_mod=AskMod, bid_mod=BidMod, name=Name} = Config) ->
    Inits = [{sbroker_queue, AskMod, AskArgs},
             {sbroker_queue, BidMod, BidArgs}],
    ReportName = report_name(Config),
    case sbroker_handlers:init(Send, Now, Inits, MeterArgs, ReportName) of
        {ok, [{_, _, Asks, _}, {_, _, Bids, BidNext}], {Meters, MNext}} ->
            Next = min(BidNext, MNext),
            NTime = Time#time{meters=Meters},
            enter_loop(Starter, NTime, Asks, Bids, Next, Config);
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

enter_loop(Starter, Time, Asks, Bids, Next, Config) ->
    proc_lib:init_ack(Starter, {ok, self()}),
    Timeout = idle_timeout(Time, Next),
    idle_recv(bidding, Timeout, Time, Asks, Bids, Config).

mark(Time) ->
    Now = erlang:monotonic_time(),
    _ = self() ! {'$mark', Now},
    Time#time{now=Now, send=Now, seq=0}.

update_time(State, #time{seq=Seq, read_after=Seq} = Time, Asks, Bids, Config) ->
    Now = erlang:monotonic_time(),
    update_meter(Now, State, Time, Asks, Bids, Config);
update_time(_, #time{seq=Seq} = Time, _, _, _) ->
    Time#time{seq=Seq+1}.

update_meter(Now, _, #time{meters=[], send=Send} = Time, _, _, _) ->
    Time#time{now=Now, seq=0, empty=Send};
update_meter(Now, asking, #time{send=Send, empty=Empty} = Time, Asks, Bids,
             #config{ask_mod=AskMod} = Config) ->
    try AskMod:send_time(Asks) of
        SendTime
          when is_integer(SendTime), SendTime =< Send, SendTime >= Empty ->
            RelativeTime = Send - SendTime,
            update_meter(Now, RelativeTime, SendTime, Time, Asks, Bids, Config);
        empty ->
            RelativeTime = Send - Empty,
            update_meter(Now, RelativeTime, Empty, Time, Asks, Bids, Config);
        Other ->
            asking_return(Other, Time, Asks, Bids, Config)
    catch
        Class:Reason ->
            asking_exception(Class, Reason, Time, Asks, Bids, Config)
    end;
update_meter(Now, bidding, #time{send=Send, empty=Empty} = Time, Asks, Bids,
             #config{bid_mod=BidMod} = Config) ->
    try BidMod:send_time(Bids) of
        SendTime
          when is_integer(SendTime), SendTime =< Send, SendTime >= Empty ->
            RelativeTime = SendTime - Send,
            update_meter(Now, RelativeTime, SendTime, Time, Asks, Bids, Config);
        empty ->
            RelativeTime = Empty - Send,
            update_meter(Now, RelativeTime, Empty, Time, Asks, Bids, Config);
        Other ->
            bidding_return(Other, Time, Asks, Bids, Config)
    catch
        Class:Reason ->
            bidding_exception(Class, Reason, Time, Asks, Bids, Config)
    end.

update_meter(Now, RelativeTime, Empty,
             #time{now=Prev, send=Send, seq=Seq, meters=Meters} = Time, Asks,
             Bids, Config) ->
    ProcessDelay = (Now - Prev) div Seq,
    %% Remove one ProcessDelay to estimate time last message was received.
    %% NB: This gives correct QueueDelay of 0 when single message was received.
    QueueDelay = (Now - ProcessDelay) - Send,
    case sbroker_handlers:meters_update(QueueDelay, ProcessDelay, RelativeTime,
                                        Now, Meters, report_name(Config)) of
        {ok, NMeters, Next} ->
            Time#time{now=Now, seq=0, meters=NMeters, empty=Empty, next=Next};
        {stop, ExitReason} ->
            meter_stop(ExitReason, Asks, Bids, Config)
    end.

meter_stop(Reason, Asks, Bids,
           #config{ask_mod=AskMod, bid_mod=BidMod} = Config) ->
    Callbacks = [{sbroker_queue, AskMod, stop, Asks},
                 {sbroker_queue, BidMod, stop, Bids}],
    terminate(Reason, Callbacks, Config).

idle(State, #time{seq=0} = Time, Asks, Bids, Next, Config) ->
    Timeout = idle_timeout(Time, Next),
    idle_recv(State, Timeout, Time, Asks, Bids, Config);
idle(State, Time, Asks, Bids, Next, Config) ->
    Now = erlang:monotonic_time(),
    NTime = update_meter(Now, State, Time, Asks, Bids, Config),
    Timeout = idle_timeout(NTime, Next),
    idle_recv(State, Timeout, NTime, Asks, Bids, Config).

idle_timeout(#time{now=Now, next=Next1}, Next2) ->
    case min(Next1, Next2) of
        infinity ->
            infinity;
        Next ->
            Diff = Next-Now,
            Timeout = erlang:convert_time_unit(Diff, native, milli_seconds),
            max(Timeout, 1)
    end.

idle_recv(State, Timeout, Time, Asks, Bids, Config) ->
    receive
        Msg ->
            NTime = mark(Time),
            handle(State, Msg, NTime, Asks, Bids, infinity, Config)
    after
        Timeout ->
            NTime = mark(Time),
            timeout(State, NTime, Asks, Bids, Config)
    end.

handle(asking, Msg, Time, Asks, Bids, Next, Config) ->
    asking(Msg, Time, Asks, Bids, Next, Config);
handle(bidding, Msg, Time, Asks, Bids, Next, Config) ->
    bidding(Msg, Time, Asks, Bids, Next, Config).

asking_timeout(#time{now=Now} = Time, Asks, Bids,
               #config{ask_mod=AskMod} = Config) ->
    try AskMod:handle_timeout(Now, Asks) of
        {NAsks, Next} ->
            asking(Time, NAsks, Bids, Next, Config);
        Other ->
            asking_return(Other, Time, Asks, Bids, Config)
    catch
        Class:Reason ->
            asking_exception(Class, Reason, Time, Asks, Bids, Config)
    end.

asking(Time, Asks, Bids, Next, Config) ->
    receive
        Msg ->
            NTime = update_time(asking, Time, Asks, Bids, Config),
            asking(Msg, NTime, Asks, Bids, Next, Config)
    end.

asking({ask, Ask, Value}, #time{now=Now, send=Send} = Time, Asks, Bids, _,
       #config{ask_mod=AskMod} = Config) ->
    try AskMod:handle_in(Send, Ask, Value, Now, Asks) of
        {NAsks, Next} ->
            asking(Time, NAsks, Bids, Next, Config);
        Other ->
            asking_return(Other, Time, Asks, Bids, Config)
    catch
        Class:Reason ->
            asking_exception(Class, Reason, Time, Asks, Bids, Config)
    end;
asking({bid, Bid, BidValue} = Msg, #time{now=Now, send=Send} = Time, Asks, Bids,
       _, #config{ask_mod=AskMod} = Config) ->
    case handle_out(AskMod, Now, Asks) of
        {AskSend, Ask, AskValue, Ref, NAsks, Next} ->
            ask_settle(Time, Ref, AskSend, Ask, AskValue, Bid, BidValue),
            asking(Time, NAsks, Bids, Next, Config);
        {empty, NAsks} ->
            bidding(Msg, Time#time{empty=Send}, NAsks, Bids, infinity, Config);
        {bad_return_value, Other, NAsks} ->
            asking_return(Other, Time, NAsks, Bids, Config);
        {exception, Class, Reason, NAsks} ->
            asking_exception(Class, Reason, Time, NAsks, Bids, Config)
    end;
asking({nb_ask, Ask, _}, Time, Asks, Bids, _, Config) ->
    drop(Ask, Time),
    asking_timeout(Time, Asks, Bids, Config);
asking({nb_bid, Bid, BidValue}, #time{now=Now, send=Send} = Time, Asks, Bids, _,
       #config{ask_mod=AskMod} = Config) ->
    case handle_out(AskMod, Now, Asks) of
        {AskSend, Ask, AskValue, Ref, NAsks, Next} ->
            ask_settle(Time, Ref, AskSend, Ask, AskValue, Bid, BidValue),
            asking(Time, NAsks, Bids, Next, Config);
        {empty, NAsks} ->
            drop(Bid, Time),
            bidding(Time#time{empty=Send}, NAsks, Bids, infinity, Config);
        {bad_return_value, Other, NAsks} ->
            asking_return(Other, Time, NAsks, Bids, Config);
        {exception, Class, Reason, NAsks} ->
            asking_exception(Class, Reason, Time, NAsks, Bids, Config)
    end;
asking({dynamic_ask, Ask, Value}, Time, Asks, Bids, Next, Config) ->
    async(Ask),
    asking({ask, Ask, Value}, Time, Asks, Bids, Next, Config);
asking({dynamic_bid, Bid, BidValue}, #time{now=Now, send=Send} = Time, Asks,
       Bids, _, #config{ask_mod=AskMod} = Config) ->
    case handle_out(AskMod, Now, Asks) of
        {AskSend, Ask, AskValue, Ref, NAsks, Next} ->
            ask_settle(Time, Ref, AskSend, Ask, AskValue, Bid, BidValue),
            asking(Time, NAsks, Bids, Next, Config);
        {empty, NAsks} ->
            async(Bid),
            Msg = {bid, Bid, BidValue},
            bidding(Msg, Time#time{empty=Send}, NAsks, Bids, infinity, Config);
        {bad_return_value, Other, NAsks} ->
            asking_return(Other, Time, NAsks, Bids, Config);
        {exception, Class, Reason, NAsks} ->
            asking_exception(Class, Reason, Time, NAsks, Bids, Config)
    end;
asking({cancel, From, Tag}, #time{now=Now} = Time, Asks, Bids, _,
       #config{ask_mod=AskMod} = Config) ->
    try AskMod:handle_cancel(Tag, Now, Asks) of
        {Reply, NAsks, Next} ->
            cancelled(From, Reply),
            asking(Time, NAsks, Bids, Next, Config);
        Other ->
            asking_return(Other, Time, Asks, Bids, Config)
    catch
        Class:Reason ->
            asking_exception(Class, Reason, Time, Asks, Bids, Config)
    end;
asking(Msg, Time, Asks, Bids, Next, Config) ->
    common(Msg, asking, Time, Asks, Bids, Next, Config).

common({'$mark', Mark}, State, #time{now=Now} = Time, Asks, Bids, Next,
       Config) ->
    receive
        Msg ->
            _ = self() ! {'$mark', Now},
            NTime = Time#time{send=(Mark + Now) div 2},
            handle(State, Msg, NTime, Asks, Bids, Next, Config)
    after
        0 ->
            idle(State, Time, Asks, Bids, Next, Config)
    end;
common({'EXIT', Parent, Reason}, _, Time, Asks, Bids, _,
       #config{parent=Parent} = Config) ->
    terminate({stop, Reason}, Time, Asks, Bids, Config);
common({system, From, Msg}, State, Time, Asks, Bids, _, Config) ->
    system(From, Msg, State, Time, Asks, Bids, Config);
common({change_config, From, _}, State, Time, Asks, Bids, _, Config) ->
    config_change(From, State, Time, Asks, Bids, Config);
common({len_ask, From, _}, State, Time, Asks, Bids, _,
       #config{ask_mod=AskMod} = Config) ->
    try AskMod:len(Asks) of
        Len ->
            gen:reply(From, Len),
            timeout(State, Time, Asks, Bids, Config)
    catch
        Class:Reason ->
            asking_exception(Class, Reason, Time, Asks, Bids, Config)
    end;
common({len_bid, From, _}, State, Time, Asks, Bids, _,
        #config{bid_mod=BidMod} = Config) ->
    try BidMod:len(Bids) of
        Len ->
            gen:reply(From, Len),
            timeout(State, Time, Asks, Bids, Config)
    catch
        Class:Reason ->
            bidding_exception(Class, Reason, Time, Asks, Bids, Config)
    end;
common({_, From, get_modules}, State, #time{meters=Meters} = Time, Asks, Bids,
       _, #config{mod=Mod, ask_mod=AskMod, bid_mod=BidMod} = Config) ->
    MeterMods = [MeterMod || {MeterMod, _} <- Meters],
    gen:reply(From, lists:usort([Mod, AskMod, BidMod | MeterMods])),
    timeout(State, Time, Asks, Bids, Config);
common(timeout, State, Time, Asks, Bids, _, Config) ->
    timeout(State, Time, Asks, Bids, Config);
common(Msg, State, Time, Asks, Bids, _, Config) ->
    info_asks(Msg, State, Time, Asks, Bids, Config).

handle_out(Mod, Now, Queue) ->
    try Mod:handle_out(Now, Queue) of
        {_, _, _, Ref, NQueue, _} = Result ->
            handle_out(Ref, Result, Mod, Now, NQueue);
        {empty, _} = Result ->
            Result;
        Other ->
            {bad_return_value, Other, Queue}
    catch
        Class:Reason ->
            {exception, Class, Reason, Queue}
    end.

handle_out(Ref, Result, Mod, Now, Queue) ->
    case demonitor(Ref, [flush, info]) of
        true ->
            Result;
        false ->
            handle_out(Mod, Now, Queue)
    end.

ask_settle(#time{now=Now, send=Send}, Ref, AskSend, Ask, AskValue, Bid,
           BidValue) ->
    settle(Now, AskSend, Ref, Ask, AskValue, Send, Bid, BidValue).

info_asks(Msg, State, #time{now=Now} = Time, Asks, Bids,
          #config{ask_mod=AskMod} = Config) ->
    try AskMod:handle_info(Msg, Now, Asks) of
        {NAsks, AskNext} ->
            info_bids(Msg, State, Time, NAsks, AskNext, Bids, Config);
        Other ->
            asking_return(Other, Time, Asks, Bids, Config)
    catch
        Class:Reason ->
            asking_exception(Class, Reason, Time, Asks, Bids, Config)
    end.

info_bids(Msg, State, #time{now=Now} = Time, Asks, AskNext, Bids,
          #config{bid_mod=BidMod} = Config) ->
    try BidMod:handle_info(Msg, Now, Bids) of
        {NBids, BidNext} ->
            info_meter(Msg, State, Time, Asks, AskNext, NBids, BidNext, Config);
        Other ->
            bidding_return(Other, Time, Asks, Bids, Config)
    catch
        Class:Reason ->
            bidding_exception(Class, Reason, Time, Asks, Bids, Config)
    end.

info_meter(_, State, #time{meters=[]} = Time, Asks, AskNext, Bids, BidNext,
           Config) ->
    next(State, Time, Asks, AskNext, Bids, BidNext, Config);
info_meter(Msg, State, #time{now=Now, meters=Meters} = Time, Asks, AskNext,
           Bids, BidNext, Config) ->
    case sbroker_handlers:meters_info(Msg, Now, Meters, report_name(Config)) of
        {ok, NMeters, MeterNext} ->
            NTime = Time#time{meters=NMeters, next=MeterNext},
            next(State, NTime, Asks, AskNext, Bids, BidNext, Config);
        {stop, Reason} ->
            meter_stop(Reason, Asks, Bids, Config)
    end.

asking_return(Return, Time, Asks, Bids,
              #config{ask_mod=AskMod, bid_mod=BidMod} = Config) ->
    Reason = {bad_return_value, Return},
    Callbacks = [{sbroker_queue, AskMod, Reason, Asks},
                 {sbroker_queue, BidMod, stop, Bids}],
    terminate(Reason, Time, Callbacks, Config).

asking_exception(Class, Reason, Time, Asks, Bids,
                 #config{ask_mod=AskMod, bid_mod=BidMod} = Config) ->
    Reason2 = {Class, Reason, erlang:get_stacktrace()},
    Callbacks = [{sbroker_queue, AskMod, Reason2, Asks},
                 {sbroker_queue, BidMod, stop, Bids}],
    terminate(Reason2, Time, Callbacks, Config).

bidding_timeout(#time{now=Now} = Time, Asks, Bids,
                #config{bid_mod=BidMod} = Config) ->
    try BidMod:handle_timeout(Now, Bids) of
        {NBids, Next} ->
            bidding(Time, Asks, NBids, Next, Config);
        Other ->
            bidding_return(Other, Time, Asks, Bids, Config)
    catch
        Class:Reason ->
            bidding_exception(Class, Reason, Time, Asks, Bids, Config)
    end.

bidding(Time, Asks, Bids, Next, Config) ->
    receive
        Msg ->
            NTime = update_time(bidding, Time, Asks, Bids, Config),
            bidding(Msg, NTime, Asks, Bids, Next, Config)
    end.

bidding({bid, Bid, Value}, #time{now=Now, send=Send} = Time, Asks, Bids, _,
        #config{bid_mod=BidMod} = Config) ->
    try BidMod:handle_in(Send, Bid, Value, Now, Bids) of
        {NBids, Next} ->
            bidding(Time, Asks, NBids, Next, Config);
        Other ->
            bidding_return(Other, Time, Asks, Bids, Config)
    catch
        Class:Reason ->
            bidding_exception(Class, Reason, Time, Asks, Bids, Config)
    end;
bidding({ask, Ask, AskValue} = Msg, #time{now=Now, send=Send} = Time, Asks,
        Bids, _, #config{bid_mod=BidMod} = Config) ->
    case handle_out(BidMod, Now, Bids) of
        {BidSend, Bid, BidValue, Ref, NBids, Next} ->
            bid_settle(Time, Ref, Ask, AskValue, BidSend, Bid, BidValue),
            bidding(Time, Asks, NBids, Next, Config);
        {empty, NBids} ->
            asking(Msg, Time#time{empty=Send}, Asks, NBids, infinity, Config);
        {bad_return_value, Other, NBids} ->
            bidding_return(Other, Time, Asks, NBids, Config);
        {exception, Class, Reason, NBids} ->
            bidding_exception(Class, Reason, Time, Asks, NBids, Config)
    end;
bidding({nb_bid, Bid, _}, Time, Asks, Bids, _, Config) ->
    drop(Bid, Time),
    bidding_timeout(Time, Asks, Bids, Config);
bidding({nb_ask, Ask, AskValue}, #time{now=Now, send=Send} = Time, Asks, Bids,
        _, #config{bid_mod=BidMod} = Config) ->
    case handle_out(BidMod, Now, Bids) of
        {BidSend, Bid, BidValue, Ref, NBids, Next} ->
            bid_settle(Time, Ref, Ask, AskValue, BidSend, Bid, BidValue),
            bidding(Time, Asks, NBids, Next, Config);
        {empty, NBids} ->
            drop(Ask, Time),
            asking(Time#time{empty=Send}, Asks, NBids, infinity, Config);
        {bad_return_value, Other, NBids} ->
            bidding_return(Other, Time, Asks, NBids, Config);
        {exception, Class, Reason, NBids} ->
            bidding_exception(Class, Reason, Time, Asks, NBids, Config)
    end;
bidding({dynamic_bid, Bid, Value}, Time, Asks, Bids, Next, Config) ->
    async(Bid),
    bidding({bid, Bid, Value}, Time, Asks, Bids, Next, Config);
bidding({dynamic_ask, Ask, AskValue}, #time{now=Now, send=Send} = Time, Asks,
        Bids, _, #config{bid_mod=BidMod} = Config) ->
    case handle_out(BidMod, Now, Bids) of
        {BidSend, Bid, BidValue, Ref, NBids, Next} ->
            bid_settle(Time, Ref, Ask, AskValue, BidSend, Bid, BidValue),
            bidding(Time, Asks, NBids, Next, Config);
        {empty, NBids} ->
            async(Ask),
            Msg = {ask, Ask, AskValue},
            asking(Msg, Time#time{empty=Send}, Asks, NBids, infinity, Config);
        {bad_return_value, Other, NBids} ->
            bidding_return(Other, Time, Asks, NBids, Config);
        {exception, Class, Reason, NBids} ->
            bidding_exception(Class, Reason, Time, Asks, NBids, Config)
    end;
bidding({cancel, From, Tag}, #time{now=Now} = Time, Asks, Bids, _,
        #config{bid_mod=BidMod} = Config) ->
    try BidMod:handle_cancel(Tag, Now, Bids) of
        {Reply, NBids, Next} ->
            cancelled(From, Reply),
            bidding(Time, Asks, NBids, Next, Config);
        Other ->
            bidding_return(Other, Time, Asks, Bids, Config)
    catch
        Class:Reason ->
            bidding_exception(Class, Reason, Time, Asks, Bids, Config)
    end;
bidding(Msg, Time, Asks, Bids, Next, Config) ->
    common(Msg, bidding, Time, Asks, Bids, Next, Config).

bid_settle(#time{now=Now, send=Send}, Ref, Ask, AskValue, BidSend, Bid,
           BidValue) ->
    settle(Now, Send, Ref, Ask, AskValue, BidSend, Bid, BidValue).

bidding_return(Return, Time, Asks, Bids,
               #config{ask_mod=AskMod, bid_mod=BidMod} = Config) ->
    Reason = {bad_return_value, Return},
    Callbacks = [{sbroker_queue, AskMod, stop, Asks},
                 {sbroker_queue, BidMod, Reason, Bids}],
    terminate(Reason, Time, Callbacks, Config).

bidding_exception(Class, Reason, Time, Asks, Bids,
                  #config{ask_mod=AskMod, bid_mod=BidMod} = Config) ->
    Reason2 = {Class, Reason, erlang:get_stacktrace()},
    Callbacks = [{sbroker_queue, AskMod, stop, Asks},
                 {sbroker_queue, BidMod, Reason2, Bids}],
    terminate(Reason2, Time, Callbacks, Config).

settle(Now, AskSend, Ref, Ask, AskValue, BidSend, Bid, BidValue) ->
    RelativeTime = AskSend - BidSend,
    %% Bid always messaged first.
    gen:reply(Bid, {go, Ref, AskValue, RelativeTime, Now - BidSend}),
    gen:reply(Ask, {go, Ref, BidValue, -RelativeTime, Now - AskSend}).

drop(From, #time{now=Now, send=Send}) ->
    sbroker_queue:drop(From, Send, Now).

async({_, Tag} = From) ->
    gen:reply(From, {await, Tag, self()}).

cancelled(dirty, _) ->
    ok;
cancelled(From, Reply) ->
    gen:reply(From, Reply).

config_change(From, State, Time, Asks, Bids, Config) ->
    case config_change(Config) of
        {ok, Change} ->
            gen:reply(From, ok),
            change(State, Change, Time, Asks, Bids, Config);
        ignore  ->
            gen:reply(From, ok),
            timeout(State, Time, Asks, Bids, Config);
        {error, Reason} ->
            gen:reply(From, {error, Reason}),
            timeout(State, Time, Asks, Bids, Config)
    end.

config_change(#config{mod=Mod, args=Args}) ->
    try Mod:init(Args) of
        {ok, {{AskMod, AskArgs}, {BidMod, BidArgs}, MeterArgs}}
          when is_list(MeterArgs) ->
            config_meters(AskMod, AskArgs, BidMod, BidArgs, MeterArgs);
        ignore ->
            ignore;
        Other ->
            {error, {bad_return_value, Other}}
    catch
        Class:Reason ->
            {error, {Class, Reason, erlang:get_stacktrace()}}
    end.

config_meters(AskMod, AskArgs, BidMod, BidArgs, MeterArgs) ->
    case check_meters(MeterArgs) of
        ok ->
            {ok, {AskMod, AskArgs, BidMod, BidArgs, MeterArgs}};
        {error, Reason} ->
            {error, {Reason, MeterArgs}}
    end.

check_meters(Meters) ->
    check_meters(Meters, #{}).

check_meters([{Meter, _} | Rest], Acc) ->
    case maps:is_key(Meter, Acc) of
        true ->
            {error, {duplicate_meter, Meter}};
        false ->
            check_meters(Rest, maps:put(Meter, meter, Acc))
    end;
check_meters([], _) ->
    ok;
check_meters(_, _) ->
    {error, bad_return_value}.

code_change([State, #time{now=Now, send=Send, meters=Meters} = Time, Asks, Bids,
             #config{ask_mod=AskMod, bid_mod=BidMod} = Config], Mod, OldVsn,
            Extra) ->
    Callbacks = [{sbroker_queue, AskMod, Asks, infinity},
                 {sbroker_queue, BidMod, Bids, infinity}],
    NCallbacks = sbroker_handlers:code_change(Send, Now, Callbacks, Meters, Mod,
                                              OldVsn, Extra),
    {[{sbroker_queue, AskMod, NAsks, _},
     {sbroker_queue, BidMod, NBids, _}], {NMeters, MNext}} = NCallbacks,
    [State, Time#time{meters=NMeters, next=MNext}, NAsks, NBids, Config].

change(State, {NAskMod, AskArgs, NBidMod, BidArgs, MeterArgs},
       #time{now=Now, send=Send, meters=Meters} = Time, Asks, Bids,
       #config{ask_mod=AskMod, bid_mod=BidMod} = Config) ->
    Inits = [{sbroker_queue, AskMod, Asks, NAskMod, AskArgs},
             {sbroker_queue, BidMod, Bids, NBidMod, BidArgs}],
    Name = report_name(Config),
    case sbroker_handlers:config_change(Send, Now, Inits, Meters, MeterArgs,
                                        Name) of
        {ok, [{_, _, NAsks, AskNext}, {_, _, NBids, BidNext}],
         {NMeters, MeterNext}} ->
            NTime = Time#time{meters=NMeters, next=MeterNext},
            NConfig = Config#config{ask_mod=NAskMod, bid_mod=NBidMod},
            next(State, NTime, NAsks, AskNext, NBids, BidNext, NConfig);
        {stop, Reason} ->
            exit(Reason)
    end.

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

format_state(asking) ->
    ask;
format_state(bidding) ->
    ask_r.

terminate(Reason, Callbacks, Config) ->
    Name = report_name(Config),
    {stop, NReason} = sbroker_handlers:terminate(Reason, Callbacks, [], Name),
    exit(NReason).

terminate(Reason, #time{meters=Meters}, Callbacks, Config) ->
    Name = report_name(Config),
    {stop, NReason} = sbroker_handlers:terminate(Reason, Callbacks, Meters,
                                                 Name),
    exit(NReason).

terminate(Reason, Time, Asks, Bids,
          #config{ask_mod=AskMod, bid_mod=BidMod} = Config) ->
    Callbacks = [{sbroker_queue, AskMod, stop, Asks},
                 {sbroker_queue, BidMod, stop, Bids}],
    terminate(Reason, Time, Callbacks, Config).

report_name(#config{name=Pid, mod=Mod}) when is_pid(Pid) ->
    {Mod, Pid};
report_name(#config{name=Name}) ->
    Name.
