-module(sbroker_legacy).

-export([monotonic_time/0]).
-export([monotonic_time/1]).

monotonic_time() ->
    {Mega, Sec, Micro} = erlang:now(),
    ((Mega * 1000000 + Sec) * 1000000) + Micro.

monotonic_time(native) ->
    monotonic_time();
monotonic_time(nano_seconds) ->
    1000 * monotonic_time();
monotonic_time(micro_seconds) ->
    monotonic_time();
monotonic_time(milli_seconds) ->
    {Mega, Sec, Micro} = erlang:now(),
    (((Mega * 1000000) + Sec) * 1000) + (Micro div 1000);
monotonic_time(seconds) ->
    {Mega, Sec, _} = erlang:now(),
    (Mega * 1000000) + Sec;
monotonic_time(1000000) ->
    monotonic_time();
monotonic_time(N) when is_integer(N) and N > 0 ->
    (N * monotonic_time()) div 1000000.
