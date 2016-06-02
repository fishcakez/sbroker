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
%% @doc Server for storing the `ask' and `ask_r' values for load balanacing
%% processes with `sbetter'.
-module(sbetter_server).

-behaviour(gen_server).

%% public API

-export([register/3]).
-export([unregister/1]).
-export([update/3]).

%% private API

-export([start_link/0]).
-export([lookup/2]).

%% gen_server API

-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([code_change/3]).
-export([terminate/2]).

%% macros

-define(TIMEOUT, 5000).

%% public API

%% @doc Register the local process `Pid' with the server and sets the `ask' and
%% `ask_r' values (such as sojourn times) as integer values `AskValue' and
%% `AskRValue'.
%%
%% Returns `true' if the process is successfully registered, or `false' if
%% already registered.
-spec register(Pid, AskValue, AskRValue) -> Result when
      Pid :: pid(),
      AskValue :: integer(),
      AskRValue :: integer(),
      Result :: boolean().
register(Pid, AskValue, BidValue)
  when is_pid(Pid), node(Pid) == node(),
       is_integer(AskValue), is_integer(BidValue) ->
    gen_server:call(?MODULE, {register, Pid, AskValue, BidValue}, ?TIMEOUT).

%% @doc Unregister the process `Pid' with the server.
%%
%% The server will synchronously unlink from `Pid'.
-spec unregister(Pid) -> true when
      Pid :: pid().
unregister(Pid) when is_pid(Pid), node(Pid) == node() ->
    gen_server:call(?MODULE, {unregister, Pid}, ?TIMEOUT).

%% @doc Update the `ask' and `ask_r'  with values (such as sojourn times) as
%% integer values `AskValue' and `AskRValue' for `Pid'.
%%
%% Returns `true' if the values were updated, or `false' if `Pid' is not
%% registered with the server.
-spec update(Pid, AskValue, AskRValue) -> Result when
      Pid :: pid(),
      AskValue :: integer(),
      AskRValue :: integer(),
      Result :: boolean().
update(Pid, AskValue, BidValue)
  when is_integer(AskValue), is_integer(BidValue) ->
    ets:update_element(?MODULE, {Pid, ask}, {2, AskValue}) andalso
    ets:update_element(?MODULE, {Pid, bid}, {2, BidValue}).

%% private API

%% @private
-spec start_link() -> {ok, Pid} when
      Pid :: pid().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, ?MODULE, []).

%% @private
-spec lookup(Pid, Key) -> SojournTime when
      Pid :: pid(),
      Key :: ask | ask_r,
      SojournTime :: non_neg_integer().
lookup(Pid, ask) ->
    ets:update_counter(?MODULE, {Pid, ask}, {2, 0});
lookup(Pid, ask_r) ->
    ets:update_counter(?MODULE, {Pid, bid}, {2, 0}).

%% gen_server API

%% @private
init(Table) ->
    _ = process_flag(trap_exit, true),
    Table = ets:new(Table,
                    [named_table, set, public, {write_concurrency, true}]),
    {ok, Table}.

%% @private
handle_call({register, Pid, AskValue, BidValue}, _, Table) ->
    link(Pid),
    {reply, insert_new(Table, Pid, AskValue, BidValue), Table};
handle_call({unregister, Pid}, _, Table) ->
    delete(Table, Pid),
    unlink(Pid),
    {reply, true, Table};
handle_call(Call, _, Table) ->
    {stop, {bad_call, Call}, Table}.

%% @private
handle_cast(Cast, Table) ->
    {stop, {bad_cast, Cast}, Table}.

%% @private
handle_info({'EXIT', Pid, _}, Table) ->
    delete(Table, Pid),
    {noreply, Table};
handle_info(Msg, Table) ->
    error_logger:error_msg("sbetter_server received unexpected message: ~p~n",
                           [Msg]),
    {noreply, Table}.

%% @private
code_change(_, State, _) ->
    {ok, State}.

%% @private
terminate(_, _) ->
    ok.

%% Helpers

insert_new(Table, Pid, AskValue, BidValue) ->
    ets:insert_new(Table, {{Pid, ask}, AskValue}) andalso
    ets:insert_new(Table, {{Pid, bid}, BidValue}).

delete(Table, Pid) ->
    ets:delete(Table, {Pid, ask}),
    ets:delete(Table, {Pid, bid}).
