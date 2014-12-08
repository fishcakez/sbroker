-module(squeue_naive).

-behaviour(squeue).

-export([init/1]).
-export([handle_time/3]).
-export([handle_join/1]).

-spec init(Args) -> undefined when
      Args :: any().
init(_Args) ->
    undefined.

-spec handle_time(Time, Q, undefined) -> {[], Q, undefined} when
      Time :: non_neg_integer(),
      Q :: queue:queue().
handle_time(_Time, Q, undefined) ->
    {[], Q, undefined}.

-spec handle_join(undefined) -> undefined.
handle_join(undefined) ->
    undefined.
