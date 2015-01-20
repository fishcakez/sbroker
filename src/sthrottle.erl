%% @doc
%% This modules provides a concurrency limiting service. A process joins a queue
%% and remains there until the number of active processes goes below the limit.
%% The queue can be actively managed using an `squeue' callback module, and
%% passively managed using head or tail drop. Processes that die while in the
%% queue are automatically removed and active processes that die are replaced by
%% processes in the queue. The concurrency limit can be altered using the built
%% in algorithm and/or a custom (manual) feedback loop.
%%
%% To join the queue a process calls `ask/1', which will block until the process
%% can start the task. Once the task is completed `done/2' releases the lock.
%%
%% To increase the concurrency limit (up to the maximum) call `positive/1', and
%% to decrease (up to the minimum) call `negative/1'.
%%
%% To use the built in feedback loop call `signal/3' on the result of another
%% queue attempt (e.g. `sbroker:ask/1'). This will reduce the concurrency limit
%% when processes are dropped. It will also increase the concurrency limit when
%% two queue attempts in a row are successful with a sojourn time of 0.
-module(sthrottle).

-behaviour(gen_fsm).

-compile({no_auto_import, [put/2, erase/1]}).

%% public api

-export([ask/1]).
-export([nb_ask/1]).
-export([async_ask/1]).
-export([cancel/2]).
-export([done/2]).

-export([signal/3]).
-export([erase/1]).

-export([positive/1]).
-export([negative/1]).

-export([start_link/0]).
-export([start_link/1]).
-export([start_link/4]).
-export([start_link/5]).

%% test api

-export([force_timeout/1]).

%% gen_fsm api

-export([init/1]).
-export([grow/2]).
-export([grow/3]).
-export([steady/2]).
-export([steady/3]).
-export([shrink/2]).
-export([shrink/3]).
-export([handle_event/3]).
-export([handle_sync_event/4]).
-export([handle_info/3]).
-export([code_change/4]).
-export([terminate/3]).

%% types

-type throttle() :: pid() | atom() | {atom(), node()} | {global, any()}
    | {via, module(), any()}.
-type name() :: {local, atom()} | {global, any()} | {via, module(), any()}.
-type queue_spec() :: {module(), any(), out | out_r,
                       non_neg_integer() | infinity, drop | drop_r}.

-record(config, {interval :: pos_integer(),
                 next_timeout = 0 :: non_neg_integer(),
                 timer = make_ref() :: reference(),
                 min :: non_neg_integer(),
                 max :: non_neg_integer() | infinity,
                 out :: out | out_r,
                 drop_out :: out | out_r,
                 squeue_size :: non_neg_integer() | infinity}).

-record(state, {config :: #config{},
                len = 0 :: non_neg_integer(),
                squeue :: squeue:squeue(),
                size :: non_neg_integer(),
                active = gb_sets:new() :: gb_sets:set(reference())}).

%% public api

%% @doc Tries to gain access to a work lock. Returns
%% `{go, Ref, Pid, SojournTime}' on success or `{drop, SojournTime}' on failure.
%%
%% `Ref' is the lock reference, which is a `reference()'. `Pid' is the `pid()'
%% of the throttle. `SojournTime' is the time spent in the queue in
%% milliseconds.
%%
%% A process should stop the task if `Pid' exits as the lock is lost. Usually
%% this can achieved by using a `rest_for_one' or `one_for_all' supervisor that
%% will shutdown the worker if the throttle exits. If this is not in place a
%% monitor, or link (warning: the throttle does not trap exits), can be used.
%%
%% The `Pid' should be used as the `Throttle' in future calls that use the lock,
%% such as `done/2' and `signal/3'.
-spec ask(Throttle) -> {go, Ref, Pid, SojournTime} | {drop, SojournTime} when
      Throttle :: throttle(),
      Ref :: reference(),
      Pid :: pid(),
      SojournTime :: non_neg_integer().
ask(Throttle) ->
    gen_fsm:sync_send_event(Throttle, ask, infinity).

%% @doc Tries to match gain access to a work lock but does not enqueue the
%% request if no lock is available immediately. Returns `{go, Ref, Pid, 0}' on
%% a success or `{retry, 0}'.
%%
%% `Ref' is the lock reference, which is a `reference()'. `Pid' is the `pid()'
%% of the throttle. `0' reflects the fact that no time was spent in the queue.
%%
%% @see ask/1
-spec nb_ask(Throttle) -> {go, Ref, Pid, 0} | {retry, 0} when
      Throttle :: throttle(),
      Ref :: reference(),
      Pid :: pid().
nb_ask(Throttle) ->
    gen_fsm:sync_send_event(Throttle, nb_ask, infinity).

%% @doc Sends an asynchronous request to gain access to a work lock. Returns a
%% `reference()', `ARef', which can be used to identify the reply containing the
%% result of the request, or to cancel the request using `cancel/1'.
%%
%% The reply is of the form `{ARef, {go, Ref, Pid, SojournTime}' or
%% `{ARef, {drop, SojournTime}}'.
%%
%% Multiple asynchronous requests can be made from a single process to a
%% throttle and no guarantee is made of the order of replies. If the throttle
%% exits or is on a disconnected node there is no guarantee of
%% a reply and so the caller should take appriopriate steps to handle this
%% scenario.
%%
%% @see cancel/2
-spec async_ask(Throttle) -> ARef when
      Throttle :: throttle(),
      ARef :: reference().
async_ask(Throttle) ->
    ARef = make_ref(),
    From = {self(), ARef},
    gen_fsm:send_event(Throttle, {ask, From}),
    ARef.

%% @doc Cancels an asynchronous request. Returns `ok' on success and
%% `{error, not_found}' if the request does not exist. In the later case a
%% caller may wish to check its message queue for an existing reply.
%%
%% @see async_ask/1
-spec cancel(Throttle, ARef) -> ok | {error, not_found} when
      Throttle :: throttle(),
      ARef :: reference().
cancel(Throttle, ARef) ->
    gen_fsm:sync_send_all_state_event(Throttle, {cancel, ARef}).

%% @doc Releases the lock represented by `Ref'. Returns `ok' on success and
%% `{error, not_found}' if the request does not exist.
%%
%% @see ask/1
-spec done(Throttle, Ref) -> ok | {error, not_found} when
      Throttle :: throttle(),
      Ref :: reference().
done(Throttle, Ref) ->
    _ = erase(Ref),
    gen_fsm:sync_send_event(Throttle, {done, Ref}).

%% @doc Applies positive feedback to the throttle. Increases the concurrency
%% limit by 1, up to the maximum.
-spec positive(Throttle) -> ok when
      Throttle :: throttle().
positive(Throttle) ->
    gen_fsm:send_event(Throttle, positive).

%% @doc Applies negative feedback to the throttle. Decreases the concurrency
%% limit by 1, up to the minimum.
-spec negative(Throttle) -> ok when
      Throttle :: throttle().
negative(Throttle) ->
    gen_fsm:send_event(Throttle, negative).

%% @doc Send a signal to the throttle based on a queue attempt response. Returns
%% the response if the response is a `go' tuple. Returns a new response, which
%% might be the same, if the response is a `drop' tuple:
%%
%% `{done, SojournTime}' means the concurrency lock is lost and `SojournTime' is
%% the sojourn time from the initial response.
%%
%% `{not_found, SojournTime}' means the concurrency lock did not exist on the
%% throttle and `SojournTime' is the sojourn time from the initial response.
%%
%% This function is designed to control the concurrency limit of a throttle
%% process based on a queue attempt on a different queue (such as the result
%% of `sbroker:ask/1'), not on queue attempts on the throttle itself.
-spec signal(Throttle, Ref, Response) -> Response when
      Throttle :: throttle(),
      Ref :: reference(),
      Response :: {go, Ref2, Pid, SojournTime} | {retry, SojournTime},
      Ref2 :: reference(),
      Pid :: pid(),
      SojournTime :: non_neg_integer();
    (Throttle, Ref, Response) -> NResponse when
      Throttle :: throttle(),
      Ref :: reference(),
      Response :: {drop, SojournTime},
      SojournTime :: non_neg_integer(),
      NResponse :: {drop | done | not_found, SojournTime}.
signal(Throttle, Ref, {drop, SojournTime} = Response) ->
    _ = put(Ref, drop),
    case gen_fsm:sync_send_event(Throttle, {drop, Ref}) of
        drop ->
            Response;
        Other ->
            _ = erase(Ref),
            {Other, SojournTime}
    end;
signal(Throttle, Ref, {go, _, _, 0} = Response) ->
    case put(Ref, fast_go) of
        fast_go ->
            positive(Throttle);
        _ ->
            ok
    end,
    Response;
signal(_, Ref, {go, _, _, _} = Response) ->
    _ = put(Ref, go),
    Response;
signal(_, Ref, {retry, _} = Response) ->
    _ = put(Ref, retry),
    Response.

%% @doc Removes process dictionary entries relating to the lock `Ref'.
%%
%% `signal/3' may use the process dictionary to store state. This is cleaned up
%% by `signal/3' when it returns `{done, SojournTime}' and by `done/2'. However
%% if the throttle exits and owner of the lock does not then this
%% function should be called to prevent a leak.
%%
%% This function can also be used to forget any data used by `signal/3' while
%% the lock is still active. Future calls to `signal/3' on the same lock will
%% still work and may re-add an entry to the process dictionary.
-spec erlang:erase(Ref) -> ok when
      Ref :: reference().
erase(Ref) ->
    _ = erlang:erase({?MODULE, Ref}),
    ok.

%% @doc Starts a throttle with default limits and queues. The default
%% queue uses `squeue_timeout' with a timeout of `5000', which means that items
%% are dropped if they spend longer than 5000ms in the queue. The queue has a
%% size of `infinity' and uses `out' to dequeue items. The tick interval is
%% `200', so the active queue management timeout strategy is applied at least
%% every 200ms. The minimum (and initial) concurrency limit is `0' and the
%% maximum is `infinity'.
-spec start_link() -> {ok, Pid} | {error, Reason} when
      Pid :: pid(),
      Reason :: any().
start_link() ->
    Spec = {squeue_timeout, 5000, out, infinity, drop},
    start_link(0, infinity, Spec, 200).

%% @doc Starts a registered throttle with default limits and queue.
%% @see start_link/1
-spec start_link(Name) -> {ok, Pid} | {error, Reason} when
      Name :: name(),
      Pid :: pid(),
      Reason :: any().
start_link(Name) ->
    Spec = {squeue_timeout, 5000, out, infinity, drop},
    start_link(Name, 1, infinity, Spec, 200).

%% @doc Starts a throttle with custom limits and queue.
%%
%% The first argument, `Min' is the minimum, (and initial), concurrency limit
%% and is a `non_neg_integer()'. The second argument, `Max', is the maximum
%% concurrency limit, is a `non_neg_integer()' or `infinity' and must be greater
%% than or equal to `Min'.
%%
%% The third argument, `QueueSpec', is the queue specification for the queue.
%% Processes that call `ask/1' (or `async_ask/1') join this queue until they
%% gain a lock or are dropped. The fourth argument, `Interval', is the interval
%% in milliseconds that the queue is polled. This ensures that the active queue
%% management strategy is applied even if no processes are enqueued/dequeued.
%%
%% A queue specifcation takes the following form:
%% `{Module, Args, Out, Size, Drop}'. `Module' is the `squeue' callback module
%% and `Args' are its arguments. The queue is created using
%% `squeue:new(Module, Arg)'. `Out' defines the method of dequeuing, it is
%% either the atom `out' (dequeue items from the head, i.e. FIFO), or the
%% atom `out_r' (dequeue items from the tail, i.e. LIFO). `Size' is the maximum
%% size of the queue, it is either a `non_neg_integer()' or `infinity'. `Drop'
%% defines the strategy to take when the maximum size, `Size', of the queue is
%% exceeded. It is either the atom `drop' (drop from the head of the queue, i.e.
%% head drop) or `drop_r' (drop from the tail of the queue, i.e. tail drop)
-spec start_link(Min, Max, AskingSpec, Interval) ->
    {ok, Pid} | {error, Reason} when
      Min :: non_neg_integer(),
      Max :: non_neg_integer() | infinity,
      AskingSpec :: queue_spec(),
      Interval :: pos_integer(),
      Pid :: pid(),
      Reason :: any().
start_link(Min, Max, AskingSpec, Interval) ->
    gen_fsm:start_link(?MODULE, {Min, Max, AskingSpec, Interval}, []).

%% @doc Starts a registered throttle with custom queues.
%% @see start_link/3
-spec start_link(Name, Min, Max, AskingSpec, Interval) ->
    {ok, Pid} | {error, Reason} when
      Name :: name(),
      Min :: non_neg_integer(),
      Max :: non_neg_integer() | infinity,
      AskingSpec :: queue_spec(),
      Interval :: pos_integer(),
      Pid :: pid(),
      Reason :: any().
start_link(Name, Min, Max, AskingSpec, Interval) ->
    gen_fsm:start_link(Name, ?MODULE, {Min, Max, AskingSpec, Interval}, []).

%% test api

%% @hidden
force_timeout(Broker) ->
    gen_fsm:sync_send_all_state_event(Broker, force_timeout).

%% gen_fsm api

%% @private
init({Min, Max, {Mod, Args, Out, SSize, Drop}, Interval})
  when (is_integer(Min) andalso Min >= 0) andalso
       ((is_integer(Max) andalso Max >= Min) orelse Max =:= infinity) andalso
       (Out =:= out orelse Out =:= out_r) andalso
       ((is_integer(SSize) andalso SSize >= 0) orelse SSize =:= infinity) ->
    Config = #config{interval=Interval, min=Min, max=Max, out=Out,
                     drop_out=drop_out(Drop), squeue_size=SSize},
    {Config2, Time} = start_timer(Config),
    S = squeue:new(Time, Mod, Args),
    State = #state{config=Config2, size=Min, squeue=S},
    case Min of
        0 ->
            {ok, steady, State};
        _ ->
            {ok, grow, State}
    end.

%% @private
grow(positive, State) ->
    grow_positive(State);
grow(negative, State) ->
    grow_negative(State);
grow({ask, From}, State) ->
    grow_ask(From, State);
grow({timeout, TRef, ?MODULE}, #state{config=#config{timer=TRef}} = State) ->
    grow_timeout(State);
grow({timeout, _, ?MODULE}, State) ->
    {next_state, grow, State}.

%% @private
grow({drop, MRef}, _, #state{active=Active} = State) ->
    case gb_sets:is_element(MRef, Active) of
        true ->
            grow_drop(MRef, State);
        false ->
            {reply, not_found, grow, State}
    end;
grow(ask, From, State) ->
    grow_ask(From, State);
grow(nb_ask, From, State) ->
    grow_ask(From, State);
grow({done, MRef}, _, #state{active=Active} = State) ->
    case gb_sets:is_element(MRef, Active) of
        true ->
            grow_done(MRef, State);
        false ->
            {reply, not_found, grow, State}
    end.

%% @private
steady(positive, State) ->
    steady_positive(State);
steady(negative, State) ->
    steady_negative(State);
steady({ask, From}, State) ->
    steady_ask(From, State);
steady({timeout, TRef, ?MODULE}, #state{config=#config{timer=TRef}} = State) ->
    steady_timeout(State);
steady({timeout, _, ?MODULE}, State) ->
    {next_state, steady, State}.

%% @private
steady({drop, MRef}, _, #state{active=Active} = State) ->
    case gb_sets:is_element(MRef, Active) of
        true ->
            steady_drop(MRef, State);
        false ->
            {reply, not_found, steady, State}
    end;
steady(ask, From, State) ->
    steady_ask(From, State);
steady(nb_ask, From, State) ->
    retry(From),
    {next_state, steady, State};
steady({done, MRef}, From, #state{active=Active} = State) ->
    case gb_sets:is_element(MRef, Active) of
        true ->
            steady_done(MRef, From, State);
        false ->
            {reply, not_found, grow, State}
    end.

%% @private
shrink(positive, State) ->
    shrink_positive(State);
shrink(negative, State) ->
    shrink_negative(State);
shrink({ask, From}, State) ->
    shrink_ask(From, State);
shrink({timeout, TRef, ?MODULE}, #state{config=#config{timer=TRef}} = State) ->
    shrink_timeout(State);
shrink({timeout, _, ?MODULE}, State) ->
    {next_state, shrink, State}.

%% @private
shrink({drop, MRef}, _, #state{active=Active} = State) ->
    case gb_sets:is_element(MRef, Active) of
        true ->
            shrink_drop(MRef, State);
        false ->
            {reply, not_found, shrink, State}
    end;
shrink(ask, From, State) ->
    shrink_ask(From, State);
shrink(nb_ask, From, State) ->
    retry(From),
    {next_state, shrink, State};
shrink({done, MRef}, _, #state{active=Active} = State) ->
    case gb_sets:is_element(MRef, Active) of
        true ->
            shrink_done(MRef, State);
        false ->
            {reply, not_found, shrink, State}
    end.

%% @private
handle_event(Event, _, State) ->
    {stop, {bad_event, Event}, State}.

%% @private
handle_sync_event(force_timeout, _, StateName,
             #state{config=#config{timer=TRef}} = State) ->
    _ = erlang:cancel_timer(TRef),
    gen_fsm:send_event(self(), {timeout, TRef, ?MODULE}),
    {reply, ok, StateName, State};
handle_sync_event({cancel, ARef}, _, StateName, State) ->
    cancel(ARef, StateName, State).

%% @private
handle_info({'DOWN', MRef, _, _, _}, StateName, #state{active=Active} = State) ->
    case gb_sets:is_element(MRef, Active) of
        true when StateName =:= grow ->
            grow_down(MRef, State);
        true when StateName =:= steady ->
            steady_down(MRef, State);
        true when StateName =:= shrink ->
            shrink_down(MRef, State);
        false ->
            queue_down(MRef, StateName, State)
    end.

%% @private
code_change(_, StateName, State, _) ->
    {ok, StateName, State}.

%% @private
terminate(_, _, _) ->
    ok.

%% internal

put(Ref, State) ->
    erlang:put({?MODULE, Ref}, State).

drop_out(drop) -> out;
drop_out(drop_r) -> out_r.

start_timer(#config{interval=Interval, next_timeout=NextTimeout} = Config) ->
    TRef = gen_fsm:start_timer(Interval, ?MODULE),
    NConfig = Config#config{next_timeout=NextTimeout+Interval, timer=TRef},
    {NConfig, NextTimeout}.

read_timer(#config{next_timeout=NextTimeout, timer=TRef} = Config) ->
    case erlang:read_timer(TRef) of
        false ->
            start_timer(Config);
        Rem ->
            {Config, NextTimeout-Rem}
    end.

grow_positive(#state{config=#config{max=Max}, size=Size} = State)
  when Size < Max ->
    {next_state, grow, State#state{size=Size+1}};
grow_positive(State) ->
    {next_state, grow, State}.

steady_positive(#state{config=#config{max=Max}, size=Size} = State)
  when Size < Max ->
    steady_out(State#state{size=Size+1});
steady_positive(State) ->
    {next_state, steady, State}.

shrink_positive(#state{size=Size, active=Active} = State) ->
    NSize = Size + 1,
    NState = State#state{size=NSize},
    case gb_sets:size(Active) of
        NSize ->
            {next_state, steady, NState};
        _ ->
            {next_state, shrink, NState}
    end.

steady_out(#state{config=#config{out=Out} = Config, len=Len, squeue=S,
                  active=Active} = State) ->
    {NConfig, Time} = read_timer(Config),
    {Result, Drops, NS} = squeue:Out(Time, S),
    case Result of
        empty ->
            _ = drops(Drops),
            NState = State#state{config=NConfig, len=0, squeue=NS},
            {next_state, grow, NState};
        {SojournTime, {MRef, From}} ->
            go(From, MRef, SojournTime),
            NLen = Len - drops(Drops) - 1,
            NActive = gb_sets:insert(MRef, Active),
            NState = State#state{config=NConfig, len=NLen, squeue=NS,
                                 active=NActive},
            {next_state, steady, NState}
    end.

grow_negative(#state{config=#config{min=Min}, size=Size,
                     active=Active} = State) when Size > Min ->
    NSize = Size - 1,
    NState = State#state{size=NSize},
    case gb_sets:size(Active) of
        NSize ->
            {next_state, steady, NState};
        _ ->
            {next_state, grow, NState}
    end;
grow_negative(State) ->
    {next_state, grow, State}.

steady_negative(#state{config=#config{min=Min}, size=Size} = State)
  when Size > Min ->
    {next_state, shrink, State#state{size=Size-1}};
steady_negative(State) ->
    {next_state, steady, State}.

shrink_negative(#state{config=#config{min=Min}, size=Size} = State)
  when Size > Min ->
    {next_state, shrink, State#state{size=Size-1}};
shrink_negative(State) ->
    {next_state, shrink, State}.

grow_drop(MRef, #state{config=#config{min=Min}, size=Size,
                          active=Active} = State) when Size > Min ->
    case gb_sets:size(Active) of
        ASize when ASize > Min ->
            demonitor(MRef, [flush]),
            NSize = ASize - 1,
            NActive = gb_sets:delete(MRef, Active),
            {reply, done, steady, State#state{active=NActive, size=NSize}};
        Min ->
            {reply, drop, steady, State#state{size=Min}};
        _ ->
            {reply, drop, grow, State#state{size=Min}}
    end;
grow_drop(_, State) ->
    {reply, drop, grow, State}.

steady_drop(MRef, #state{config=#config{min=Min}, active=Active} = State) ->
    case gb_sets:size(Active) of
        ASize when ASize > Min ->
            demonitor(MRef, [flush]),
            NSize = ASize - 1,
            NActive = gb_sets:delete(MRef, Active),
            {reply, done, steady, State#state{size=NSize, active=NActive}};
        _ ->
            {reply, drop, steady, State}
    end.

shrink_drop(MRef, #state{size=Size, active=Active} = State) ->
    demonitor(MRef, [flush]),
    NActive = gb_sets:delete(MRef, Active),
    NState = State#state{active=NActive},
    case gb_sets:size(NActive) of
        Size ->
            {reply, done, steady, NState};
        _ ->
            {reply, done, shrink, NState}
    end.

grow_ask({Pid, _} = From, #state{active=Active, size=Size} = State) ->
    MRef = monitor(process, Pid),
    go(From, MRef, 0),
    NActive = gb_sets:insert(MRef, Active),
    NState = State#state{active=NActive},
    case gb_sets:size(NActive) of
        Size ->
            {next_state, steady, NState};
        _ ->
            {next_state, grow, NState}
    end.

steady_ask(From, State) ->
    {next_state, steady, in(From, State)}.

shrink_ask(From, State) ->
    {next_state, shrink, in(From, State)}.

in({Pid, _} = From, #state{config=#config{drop_out=DropOut,
                                          squeue_size=SSize} = Config,
                           len=Len, squeue=S} = State) ->
    MRef = monitor(process, Pid),
    {NConfig, Time} = read_timer(Config),
    {Drops, NS} = squeue:in(Time, {MRef, From}, S),
    case Len - drops(Drops) + 1 of
        NLen when NLen > SSize ->
            {Dropped2, NS2} = drop_out(DropOut, NS),
            State#state{config=NConfig, len=NLen-Dropped2, squeue=NS2};
        NLen ->
            State#state{config=NConfig, len=NLen, squeue=NS}
    end.

drop_out(DropOut, S) ->
    case squeue:DropOut(S) of
        {empty, Drops, NS} ->
            {drops(Drops), NS};
        {Item, Drops, NS} ->
            {drops(Drops) + drops([Item]), NS}
    end.

grow_done(MRef, #state{active=Active} = State) ->
    demonitor(MRef, [flush]),
    NActive = gb_sets:delete(MRef, Active),
    {reply, ok, grow, State#state{active=NActive}}.

steady_done(MRef, From, State) ->
    Result = steady_replace(MRef, State),
    gen_fsm:reply(From, ok),
    Result.

shrink_done(MRef, #state{size=Size, active=Active} = State) ->
    demonitor(MRef, [flush]),
    NActive = gb_sets:delete(MRef, Active),
    NState = State#state{active=NActive},
    case gb_sets:size(NActive) of
        Size ->
            {reply, ok, steady, NState};
        _ ->
            {reply, ok, shrink, NState}
    end.

steady_replace(MRef, #state{active=Active} = State) ->
    demonitor(MRef, [flush]),
    NActive = gb_sets:delete(MRef, Active),
    steady_out(State#state{active=NActive}).

grow_timeout(#state{config=Config} = State) ->
    {NConfig, _} = start_timer(Config),
    {next_state, grow, State#state{config=NConfig}}.

steady_timeout(State) ->
    {next_state, steady, timeout(State)}.

shrink_timeout(State) ->
    {next_state, shrink, timeout(State)}.

timeout(#state{config=Config, len=Len, squeue=S} = State) ->
    {NConfig, Time} = start_timer(Config),
    {Drops, NS} = squeue:timeout(Time, S),
    NLen = Len - drops(Drops),
    State#state{config=NConfig, len=NLen, squeue=NS}.

grow_down(MRef, #state{size=Size, active=Active} = State) ->
    demonitor(MRef, [flush]),
    NActive = gb_sets:delete(MRef, Active),
    NState = State#state{active=NActive},
    case gb_sets:size(Active) of
        Size ->
            {next_state, steady, NState};
        _ ->
            {next_state, grow, NState}
    end.

steady_down(MRef, State) ->
    steady_replace(MRef, State).

shrink_down(MRef, #state{size=Size, active=Active} = State) ->
    demonitor(MRef, [flush]),
    NActive = gb_sets:delete(MRef, Active),
    NState = State#state{active=NActive},
    case gb_sets:size(NActive) of
        Size ->
            {next_state, steady, NState};
        _ ->
            {next_state, shrink, NState}
    end.

queue_down(MRef, StateName, #state{config=Config, squeue=S} = State) ->
    {NConfig, Time} = read_timer(Config),
    {Drops, NS} = squeue:filter(Time, fun({MRef2, _}) -> MRef2 =/= MRef end, S),
    _ = drops(Drops),
    NLen = squeue:len(NS),
    {next_state, StateName, State#state{config=NConfig, len=NLen, squeue=NS}}.

cancel(ARef, StateName, #state{config=Config, len=Len, squeue=S} = State) ->
    {NConfig, Time} = read_timer(Config),
    Cancel = fun({MRef, {_, ARef2}}) when ARef2 =:= ARef ->
                     demonitor(MRef, [flush]),
                     false;
                (_) ->
                     true
             end,
    {Drops, NS} = squeue:filter(Time, Cancel, S),
    NLen = squeue:len(NS),
    NState = State#state{config=NConfig, len=NLen, squeue=NS},
    case Len - drops(Drops) of
        %% No items filtered, i.e. the async request not in queue
        NLen ->
            {reply, {error, not_found}, StateName, NState};
        _ ->
            {reply, ok, StateName, NState}
    end.

drops(Items) ->
    drops(Items, 0).

drops([{SojournTime, {MRef, From}} | Rest], N) ->
    demonitor(MRef, [flush]),
    gen_fsm:reply(From, {drop, SojournTime}),
    drops(Rest, N+1);
drops([], N) ->
    N.

go(From, Ref, SojournTime) ->
    gen_fsm:reply(From, {go, Ref, self(), SojournTime}).

retry(From) ->
    gen_fsm:reply(From, {retry, 0}).
