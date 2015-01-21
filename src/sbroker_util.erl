-module(sbroker_util).

-export([whereis/1]).

-type process() :: pid()| atom() | {atom(), node()} | {global, any()} |
    {via, module(), any()}.

-spec whereis(Process) -> Pid | {Name, Node} | undefined when
      Process :: process(),
      Pid :: pid(),
      Name :: atom(),
      Node :: node().
whereis(Pid) when is_pid(Pid) ->
    Pid;
whereis(Name) when is_atom(Name) ->
    erlang:whereis(Name);
whereis({Name, Node}) when is_atom(Name) andalso Node =:= node() ->
    erlang:whereis(Name);
whereis({Name, Node} = Process) when is_atom(Name) andalso is_atom(Node) ->
    Process;
whereis({global, Name}) ->
    global:whereis_name(Name);
whereis({via, Mod, Name}) ->
    Mod:whereis_name(Name).
