-module(sbroker_nb_ask).
%% @doc This modules provides utility functions for messaging a process calling
%% `sbroker:ask_r/1' on a broker. The process is looked up by calling
%% `sbroker:nb_ask/1'. If `sbroker:nb_ask/1' returns `{retry, 0}' then no
%% process is found.
%%
%% This module can be used with `via' names. For example
%% `gen_server:call({via, sbroker_nb_ask, Broker}, hello)' will make a
%% gen_server call to a process calling `sbroker:ask_r/1' on broker `Broker'.

-export([whereis_name/1]).
-export([send/2]).

%% @doc Lookup the pid of a process calling `sbroker:ask_r/1' on sbroker
%% `Broker'. Returns `Pid' if `sbroker:nb_ask/1' returns
%% `{go, Ref, Pid, SojournTime}' and `undefined' on `{retry, 0}'.
-spec whereis_name(Broker) -> Pid | undefined when
      Broker :: sbroker:broker(),
      Pid :: pid().
whereis_name(Broker) ->
    case sbroker:nb_ask(Broker) of
        {go, _, Pid, _} ->
            Pid;
        {retry, _} ->
            undefined
    end.

%% @doc Lookup the pid of a process calling `sbroker:ask_r/1' on sbroker
%% `Broker' and send `Request' to it.
%%
%% This functions exits if `sbroker:nb_ask/1' returns `{retry, 0}'.
-spec send(Broker, Request) -> ok when
      Broker :: sbroker:broker(),
      Request :: any().
send(Broker, Request) ->
    case sbroker:nb_ask(Broker) of
        {go, _, Pid, _} ->
            _ = Pid ! Request,
            ok;
        {retry, _} ->
            exit({noproc, {?MODULE, send, [Broker, Request]}})
    end.
