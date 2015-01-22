%% @doc This modules provides utility functions for messaging a process calling
%% `sbroker:ask_r/1' on a broker. The process is looked up by calling
%% `sbroker:ask/1'. If `sbroker:ask/1' returns `{drop, SojournTime}' then no
%% process is found.
%%
%% This module can be used with `via' names. For example
%% `gen_server:call({via, sbroker_ask, Broker}, hello)' will make a gen_server
%% call to a process calling `sbroker:ask_r/1' on broker `Broker'.
-module(sbroker_ask).

-export([whereis_name/1]).
-export([send/2]).

%% @doc Lookup the pid of a process calling `sbroker:ask_r/1' on sbroker
%% `Broker'. Returns `Pid' if `sbroker:ask/1' returns
%% `{go, Ref, Pid, SojournTime}', and `undefined' on `{drop, SojournTime}'.
-spec whereis_name(Broker) -> Pid | undefined when
      Broker :: sbroker:broker(),
      Pid :: pid().
whereis_name(Broker) ->
    case sbroker:ask(Broker) of
        {go, _, Pid, _} ->
            Pid;
        {drop, _} ->
            undefined
    end.

%% @doc Lookup the pid of a process calling `sbroker:ask_r/1' on sbroker
%% `Broker' and send `Request' to it.
%%
%% This functions exits if `sbroker:ask/1' returns `{drop, SojournTime}'.
-spec send(Broker, Request) -> ok when
      Broker :: sbroker:broker(),
      Request :: any().
send(Broker, Request) ->
    case sbroker:ask(Broker) of
        {go, _, Pid, _} ->
            _ = Pid ! Request,
            ok;
        {drop, _} ->
            exit({noproc, {?MODULE, send, [Broker, Request]}})
    end.
