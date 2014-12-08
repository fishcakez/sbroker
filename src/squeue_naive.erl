%% @doc Implements a naive queue management algorithm where no items are
%% dropped.
%%
%% `squeue_naive' can be used as the active queue management module in a
%% `squeue' queue. It ignores any argument given and does not queue management.
-module(squeue_naive).

-behaviour(squeue).

-export([init/1]).
-export([handle_time/3]).
-export([handle_join/1]).

%% @private
-spec init(Args) -> undefined when
      Args :: any().
init(_Args) ->
    undefined.

%% @private
-spec handle_time(Time, Q, undefined) -> {[], Q, undefined} when
      Time :: non_neg_integer(),
      Q :: queue:queue().
handle_time(_Time, Q, undefined) ->
    {[], Q, undefined}.

%% @private
-spec handle_join(undefined) -> undefined.
handle_join(undefined) ->
    undefined.
