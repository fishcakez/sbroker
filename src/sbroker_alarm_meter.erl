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
-module(sbroker_alarm_meter).

-behaviour(sbroker_meter).

-export([init/2]).
-export([handle_update/4]).
-export([handle_info/3]).
-export([config_change/3]).
-export([terminate/2]).

-record(state, {target :: non_neg_integer(),
                interval :: pos_integer(),
                alarm_id :: any(),
                status = clear :: clear | set,
                toggle_next = infinity ::  integer() | infinity}).

%% @private
-spec init(Time, {Target, Interval, AlarmId}) -> State when
      Time :: integer(),
      Target :: non_neg_integer() | infinity,
      Interval :: pos_integer(),
      AlarmId :: any(),
      State :: #state{}.
init(_, {Target, Interval, AlarmId})
  when (is_integer(Target) andalso Target >= 0 orelse
        Target =:= infinity) andalso
       is_integer(Interval) andalso Interval > 0 ->
    alarm_handler:clear_alarm(AlarmId),
    #state{target=Target, interval=Interval, alarm_id=AlarmId}.

%% @private
-spec handle_update(ProcessTime, Count, Time, State) ->
    {NState, Next} when
      ProcessTime :: non_neg_integer(),
      Count :: pos_integer(),
      Time :: integer(),
      State :: #state{},
      NState :: #state{},
      Next :: integer() | infinity.
handle_update(ProcessTime, Count, Time,
              #state{status=clear, interval=Interval, alarm_id=AlarmId,
                     toggle_next=ToggleNext} = State) ->
    case message_queue_status(ProcessTime, Count, State) of
        slow when ToggleNext =:= infinity ->
            NToggleNext = Time + Interval,
            {State#state{toggle_next=NToggleNext}, NToggleNext};
        slow when ToggleNext > Time ->
            {State, ToggleNext};
        slow ->
            alarm_handler:set_alarm({AlarmId, {message_queue_slow, self()}}),
            {State#state{status=set, toggle_next=infinity}, infinity};
        fast when ToggleNext == infinity ->
            {State, ToggleNext};
        fast ->
            {State#state{toggle_next=infinity}, infinity}
    end;
handle_update(ProcessTime, Count, Time,
              #state{status=set, interval=Interval, alarm_id=AlarmId,
                     toggle_next=ToggleNext} = State) ->
    case message_queue_status(ProcessTime, Count, State) of
        slow when ToggleNext =:= infinity ->
            {State, infinity};
        slow ->
            {State#state{toggle_next=infinity}, infinity};
        fast when ToggleNext =:= infinity ->
            NToggleNext = Time + Interval,
            {State#state{toggle_next=NToggleNext}, NToggleNext};
        fast when ToggleNext > Time ->
            {State, ToggleNext};
        fast ->
            alarm_handler:clear_alarm(AlarmId),
            {State#state{status=clear, toggle_next=infinity}, infinity}
    end.

%% @private
-spec handle_info(Msg, Time, State) -> {State, Next} when
      Msg :: any(),
      Time :: integer(),
      State :: #state{},
      Next :: integer() | infinity.
handle_info(_, Time, #state{toggle_next=ToggleNext} = State) ->
    {State, max(Time, ToggleNext)}.

%% @private
-spec config_change({Target, Interval, AlarmId}, Time, State) ->
    {NState, Next} when
      Time :: integer(),
      Target :: non_neg_integer() | infinity,
      Interval :: pos_integer(),
      AlarmId :: any(),
      State :: #state{},
      NState :: #state{},
      Next :: integer() | infinity.
config_change(Args, Time, State) ->
    #state{toggle_next=ToggleNext} = NState = change(Args, Time, State),
    {NState, ToggleNext}.

%% @private
-spec terminate(Reason, State) -> ok when
      Reason :: any(),
      State :: #state{}.
terminate(_, #state{status=set, alarm_id=AlarmId}) ->
    alarm_handler:clear_alarm(AlarmId);
terminate(_, _) ->
    ok.

%% Internal

message_queue_status(ProcessTime, Count, #state{target=Target}) ->
    case process_info(self(), message_queue_len) of
        {_, 0} ->
            fast;
        {_, Len} when (ProcessTime div Count) * Len < Target ->
            fast;
        _ ->
            slow
    end.

change({Target, NInterval, AlarmId}, Time,
       #state{alarm_id=AlarmId, interval=Interval,
              toggle_next=ToggleNext} = State)
  when is_integer(Target) andalso Target >= 0 andalso
       is_integer(NInterval) andalso NInterval > 0 ->
    NState = State#state{target=Target, interval=NInterval, alarm_id=AlarmId},
    case ToggleNext of
        infinity ->
            NState;
        _ ->
            NState#state{toggle_next=max(Time, ToggleNext+NInterval-Interval)}
    end;
change(Args, Time, #state{status=set, alarm_id=AlarmId}) ->
    alarm_handler:clear_alarm(AlarmId),
    init(Time, Args);
change(Args, Time, _) ->
    init(Time, Args).
