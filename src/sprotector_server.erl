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
%% @doc Server for storing the `ask' and `ask_r' queue min, max and drop
%% probabilities for overload protection and short circuiting for a process
%% with `sprotector'.
-module(sprotector_server).

-behaviour(gen_server).

%% public API

-export([register/3]).
-export([unregister/1]).
-export([update/4]).
-export([change/6]).

%% private API

-export([start_link/0]).
-export([ask/2]).
-export([lookup/2]).
-export([len/1]).

%% gen_server API

-export([init/1]).
-export([handle_call/3]).
-export([handle_cast/2]).
-export([handle_info/2]).
-export([code_change/3]).
-export([terminate/2]).

%% macros

-define(TIMEOUT, 5000).
-define(MAX_DROP, 16#8000000). % 2^27, phash2 default range

%% public API

%% @doc Register the local process `Pid' with the server with minimum message
%% queue length `Min' and maximum message queue length `Max'. Lookup requests
%% with `sprotector' will drop requests when above `Max' or with probabilities
%% set by `update/4' if above `Min'.
%%
%% Returns `true' if the process is successfully registered, or `false' if
%% already registered.
%%
%% A process must be registered to use `update/4'. Initial length and drop
%% probabilites are set to `0' and `0.0'. The server will synchronously link to
%% the `Pid'.
-spec register(Pid, Min, Max) -> Result when
      Pid :: pid(),
      Min :: non_neg_integer(),
      Max :: non_neg_integer() | infinity,
      Result :: boolean().
register(Pid, Min, Max)
  when (is_pid(Pid) andalso node(Pid) == node()) andalso
       (is_integer(Min) andalso Min >= 0) andalso
       ((is_integer(Max) andalso Max >= 0 andalso Max >= Min)
        orelse Max == infinity) ->
    gen_server:call(?MODULE, {register, Pid, Min, Max}, ?TIMEOUT).

%% @doc Unregister the process `Pid' with the server.
%%
%% The server will synchronously unlink from `Pid'.
-spec unregister(Pid) -> true when
      Pid :: pid().
unregister(Pid) ->
    gen_server:call(?MODULE, {unregister, Pid}, ?TIMEOUT).

%% @doc Update the queue length (`Len'), and `ask' and `ask_r' drop
%% probabilities (`AskDrop' and `AskRDrop') for `Pid'.
%%
%% `Len' must be an integer greater than or equal to `0'.
%%
%% `AskDrop' and `AskRDrop' should be floats, values below `0.0' are rounded to
%% `0.0' and above `1.0' to `1.0'.
%%
%% Returns `true' if the values were updated, or `false' if `Pid' is not
%% registered with the server.
-spec update(Pid, Len, AskDrop, AskRDrop) -> Result when
      Pid :: pid(),
      Len :: non_neg_integer(),
      AskDrop :: float(),
      AskRDrop :: float(),
      Result :: boolean().
update(Pid, Len, AskDrop, AskRDrop)
  when is_integer(Len), Len >= 0, is_float(AskDrop), is_float(AskRDrop) ->
    AskInt = drop_int(AskDrop),
    AskRInt = drop_int(AskRDrop),
    ets:update_element(?MODULE, Pid, [{4, Len}, {5, AskInt}, {6, AskRInt}]).

%% @doc Change the queue minimum to `Min' and maximum to `Max', and update the
%% queue length to `Len' and the drop probabilities to `AskDrop' and `AskRDrop'.
%%
%% This function is an atomic. Returns `true' if the values were
%% changed/updated, or `false' if `Pid' is not registered with the server.
-spec change(Pid, Min, Max, Len, AskDrop, AskRDrop) -> Result when
      Pid :: pid(),
      Min :: non_neg_integer(),
      Max :: non_neg_integer() | infinity,
      Len :: non_neg_integer(),
      AskDrop :: float(),
      AskRDrop :: float(),
      Result :: boolean().
change(Pid, Min, Max, Len, AskDrop, AskRDrop)
  when (is_pid(Pid) andalso node(Pid) == node()) andalso
       (is_integer(Min) andalso Min >= 0) andalso
       ((is_integer(Max) andalso Max >= 0 andalso Max >= Min)
        orelse Max == infinity) andalso
       is_integer(Len) andalso Len >= 0 andalso
       is_float(AskDrop) andalso is_float(AskRDrop) ->
    NMax = max_int(Max),
    AskInt = drop_int(AskDrop),
    AskRInt = drop_int(AskRDrop),
    Updates = [{2, Min}, {3, NMax}, {4, Len}, {5, AskInt}, {6, AskRInt}],
    ets:update_element(?MODULE, Pid, Updates).

%% private API

%% @private
-spec start_link() -> {ok, Pid} when
      Pid :: pid().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, ?MODULE, []).

%% @private
-spec ask(Pid, Key) -> Result when
      Pid :: pid(),
      Key :: ask | ask_r,
      Result :: go | drop.
ask(Pid, ask) ->
    do_ask(Pid, [{2, 0}, {3, 0}, {4, 1}, {5, 0}]);
ask(Pid, ask_r) ->
    do_ask(Pid, [{2, 0}, {3, 0}, {4, 1}, {6, 0}]).

%% @private
-spec lookup(Pid, Key) -> Drop when
      Pid :: pid(),
      Key :: ask | ask_r,
      Drop :: float().
lookup(Pid, ask) ->
    do_lookup(Pid, 5);
lookup(Pid, ask_r) ->
    do_lookup(Pid, 6).

%% @private
-spec len(Pid) -> Len when
      Pid :: pid(),
      Len :: non_neg_integer().
len(Pid) ->
    ets:lookup_element(?MODULE, Pid, 4).

%% gen_server API

%% @private
init(Table) ->
    _ = process_flag(trap_exit, true),
    Table = ets:new(Table,
                    [named_table, set, public, {write_concurrency, true}]),
    {ok, Table}.

%% @private
handle_call({register, Pid, Min, Max}, _, Table) ->
    link(Pid),
    NMax = max_int(Max),
    {reply, ets:insert_new(Table, {Pid, Min, NMax, 0, 0, 0}), Table};
handle_call({unregister, Pid}, _, Table) ->
    ets:delete(Table, Pid),
    unlink(Pid),
    {reply, true, Table};
handle_call(Call, _, Table) ->
    {stop, {bad_call, Call}, Table}.

%% @private
handle_cast(Cast, Table) ->
    {stop, {bad_cast, Cast}, Table}.

%% @private
handle_info({'EXIT', Pid, _}, Table) ->
    ets:delete(Table, Pid),
    {noreply, Table};
handle_info(Msg, Table) ->
    error_logger:error_msg("sprotector_server received unexpected message: ~p~n",
                           [Msg]),
    {noreply, Table}.

%% @private
code_change(_, State, _) ->
    {ok, State}.

%% @private
terminate(_, _) ->
    ok.

%% Helpers

max_int(infinity) ->
    -1;
max_int(Int) ->
    Int.

drop_int(1.0) ->
    ?MAX_DROP;
drop_int(0.0) ->
    0;
drop_int(Drop) ->
    % Drop could < 0.0 or > 1.0.
    max(0, min(?MAX_DROP, erlang:trunc(Drop * ?MAX_DROP))).

do_ask(Pid, Updates) ->
    [Min, Max, Len, DropInt] = ets:update_counter(?MODULE, Pid, Updates),
    handle_ask(Min, Max, Len, DropInt).

handle_ask(_, Max, Len, _) when Len > Max, Max =/= -1 ->
    drop;
handle_ask(Min, _, Len, ?MAX_DROP) when Len > Min ->
    drop;
handle_ask(Min, _, Len, DropInt) when Len > Min, DropInt > 0 ->
    case erlang:phash2({self(), make_ref()}, ?MAX_DROP) of
        Hash when Hash < DropInt ->
            drop;
        _ ->
            go
    end;
handle_ask(_, _, _, _) ->
    go.

do_lookup(Pid, Element) ->
    ets:lookup_element(?MODULE, Pid, Element) / ?MAX_DROP.
