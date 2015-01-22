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
%% name on another node is not supported for use with `via'.
-module(sscheduler).

-export([whereis_name/1]).
-export([send/2]).

%% @doc Lookup the pid or process name of one element, selected based on the
%% scheduler id, in a tuple of process names. Returns `{atom(), node()}' if the
%% chosen element is a locally registered name on another node, or the `pid()'
%% of the process associated with the name. If no process is associated with the
%% name returns `undefined'.
-spec whereis_name(Processes) -> Process | undefined when
      Processes :: tuple(),
      Process :: pid() | {atom(), node()}.
whereis_name({}) ->
    undefined;
whereis_name(Tuple) ->
    Size = tuple_size(Tuple),
    Scheduler = erlang:system_info(scheduler_id),
    N = (Scheduler rem Size) + 1,
    Name = element(N, Tuple),
    sbroker_util:whereis(Name).

%% @doc Send a message to one element, selected based on the scheduler id, in a
%% tuple of process names. Returns `ok' if the element chosen is a `pid()'.
%% a locally registered name on another node, or a process is associated with
%% the name. Otherwise exits.
-spec send(Throttle, Msg) -> ok when
      Throttle :: tuple(),
      Msg :: any().
send(Brokers, Msg) ->
    case whereis_name(Brokers) of
        undefined ->
            exit({noproc, {?MODULE, send, [Brokers, Msg]}});
        Process ->
            _ = Process ! Msg,
            ok
    end.
