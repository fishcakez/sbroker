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
%% @doc This modules provides utility functions for messaging a process calling
%% `sbroker:ask/1' on a broker. The process is looked up by calling
%% `sbroker:ask_r/1'. If `sbroker:ask_r/1' returns `{drop, SojournTime}' then no
%% process is found.
%%
%% This module can be used with `via' names. For example
%% `gen_server:call({via, sbroker_ask_r, Broker}, hello)' will make a gen_server
%% call to a process calling `sbroker:ask/1' on broker `Broker'.
-module(sbroker_ask_r).

-export([whereis_name/1]).
-export([send/2]).

%% @doc Lookup the pid of a process calling `sbroker:ask/1' on sbroker `Broker'.
%% Returns `Pid' if `sbroker:ask_r/1' returns `{go, Ref, Pid, SojournTime}', and
%% `undefined' on `{drop, SojournTime}'.
-spec whereis_name(Broker) -> Pid | undefined when
      Broker :: sbroker:broker(),
      Pid :: pid().
whereis_name(Broker) ->
    case sbroker:ask_r(Broker) of
        {go, _, Pid, _} ->
            Pid;
        {drop, _} ->
            undefined
    end.

%% @doc Lookup the pid of a process calling `sbroker:ask/1' on sbroker `Broker'
%% and send `Request' to it.
%%
%% This functions exits if `sbroker:ask_r/1' returns `{drop, SojournTime}'.
-spec send(Broker, Request) -> ok when
      Broker :: sbroker:broker(),
      Request :: any().
send(Broker, Request) ->
    case sbroker:ask_r(Broker) of
        {go, _, Pid, _} ->
            _ = Pid ! Request,
            ok;
        {drop, _} ->
            exit({noproc, {?MODULE, send, [Broker, Request]}})
    end.
