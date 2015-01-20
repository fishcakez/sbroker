%% @doc Implements a naive queue management algorithm where no items are
%% dropped.
%%
%% `squeue_naive' can be used as the active queue management module in a
%% `squeue' queue. It ignores any argument given and does not queue management.
-module(squeue_naive).

-behaviour(squeue).

-export([init/1]).
-export([handle_timeout/3]).
-export([handle_in/3]).
-export([handle_out/3]).
-export([handle_join/3]).

%% @private
-spec init(Args) -> undefined when
      Args :: any().
init(_Args) ->
    undefined.

%% @private
-ifdef(LEGACY_TYPES).
-spec handle_timeout(Time, Q, undefined) -> {[], Q, undefined} when
      Time :: non_neg_integer(),
      Q :: queue().
-else.
-spec handle_timeout(Time, Q, undefined) -> {[], Q, undefined} when
      Time :: non_neg_integer(),
      Q :: queue:queue().
-endif.
handle_timeout(_Time, Q, undefined) ->
    {[], Q, undefined}.

%% @private
-ifdef(LEGACY_TYPES).
-spec handle_in(Time, Q, undefined) -> {[], Q, undefined} when
      Time :: non_neg_integer(),
      Q :: queue().
-else.
-spec handle_in(Time, Q, undefined) -> {[], Q, undefined} when
      Time :: non_neg_integer(),
      Q :: queue:queue().
-endif.
handle_in(_Time, Q, undefined) ->
    {[], Q, undefined}.

%% @private
-ifdef(LEGACY_TYPES).
-spec handle_out(Time, Q, undefined) -> {[], Q, undefined} when
      Time :: non_neg_integer(),
      Q :: queue().
-else.
-spec handle_out(Time, Q, undefined) -> {[], Q, undefined} when
      Time :: non_neg_integer(),
      Q :: queue:queue().
-endif.
handle_out(_Time, Q, undefined) ->
    {[], Q, undefined}.

%% @private
-ifdef(LEGACY_TYPES).
-spec handle_join(Time, Q, undefined) -> {[], Q, undefined} when
      Time :: non_neg_integer(),
      Q :: queue().
-else.
-spec handle_join(Time, Q, undefined) -> {[], Q, undefined} when
      Time :: non_neg_integer(),
      Q :: queue:queue().
-endif.
handle_join(_Time, Q, undefined) ->
    {[], Q, undefined}.
