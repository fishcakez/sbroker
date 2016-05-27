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
%% @doc Sets a SASL `alarm_handler' alarm when the process's message queue is
%% slow for an interval. Once the message queue becomes fast for an interval the
%% alarm is cleared.
%%
%% `sbroker_alarm_meter' can be used as the `sbroker_meter' in a `sbroker' or
%% a `sregulator'. Its argument is of the form:
%% ```
%% {Target :: non_neg_integer(), Interval :: pos_integer(), AlarmId :: any()}
%% '''
%% `Target' is the target sojourn time of the message queue in milliseconds. The
%% meter will set the alarm `AlarmId' in `alarm_handler' if the sojourn time of
%% the message queue is above the target for `Interval' milliseconds. Once set
%% if the sojourn time is below `Target'  for `Interval' milliseconds the alarm
%% is cleared. The description of the alarm is `{message_queue_slow, Pid}' where
%% `Pid' is the `pid()' of the process.
-module(sbroker_alarm_meter).

-behaviour(sbroker_meter).

-export([init/2]).
-export([handle_update/5]).
-export([handle_info/3]).
-export([code_change/4]).
-export([config_change/3]).
-export([terminate/2]).

-record(state, {target :: non_neg_integer(),
                interval :: pos_integer(),
                alarm_id :: any(),
                status = clear :: clear | set,
                toggle_next = infinity :: integer() | infinity}).

%% @private
-spec init(Time, {Target, Interval, AlarmId}) -> {State, infinity} when
      Time :: integer(),
      Target :: non_neg_integer(),
      Interval :: pos_integer(),
      AlarmId :: any(),
      State :: #state{}.
init(_, {Target, Interval, AlarmId}) ->
    alarm_handler:clear_alarm(AlarmId),
    State = #state{target=sbroker_util:sojourn_target(Target),
                   interval=sbroker_util:interval(Interval), alarm_id=AlarmId},
    {State, infinity}.

%% @private
-spec handle_update(QueueDelay, ProcessDelay, RelativeTime, Time, State) ->
    {NState, Next} when
      QueueDelay :: non_neg_integer(),
      ProcessDelay :: non_neg_integer(),
      RelativeTime :: integer(),
      Time :: integer(),
      State :: #state{},
      NState :: #state{},
      Next :: integer() | infinity.
handle_update(QueueDelay, _, _, Time,
              #state{status=clear, interval=Interval, alarm_id=AlarmId,
                     toggle_next=ToggleNext} = State) ->
    case message_queue_status(QueueDelay, State) of
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
handle_update(QueueDelay, _, _, Time,
              #state{status=set, interval=Interval, alarm_id=AlarmId,
                     toggle_next=ToggleNext} = State) ->
    case message_queue_status(QueueDelay, State) of
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
-spec code_change(OldVsn, Time, State, Extra) -> {NState, Next} when
      OldVsn :: any(),
      Time :: integer(),
      State :: #state{},
      Extra :: any(),
      NState :: #state{},
      Next :: integer() | infinity.
code_change(_, Time, #state{toggle_next=ToggleNext} = State, _) ->
    {State, max(Time, ToggleNext)}.

%% @private
-spec config_change({Target, Interval, AlarmId}, Time, State) ->
    {NState, Next} when
      Target :: non_neg_integer(),
      Interval :: pos_integer(),
      AlarmId :: any(),
      Time :: integer(),
      State :: #state{},
      NState :: #state{},
      Next :: integer() | infinity.
config_change({Target, NInterval, AlarmId}, Time,
              #state{alarm_id=AlarmId, interval=Interval,
                     toggle_next=ToggleNext} = State) ->
    NTarget = sbroker_util:sojourn_target(Target),
    NInterval2 = sbroker_util:interval(NInterval),
    NState = State#state{target=NTarget, interval=NInterval2, alarm_id=AlarmId},
    case ToggleNext of
        infinity ->
            {NState, infinity};
        _ ->
            NToggleNext = ToggleNext+NInterval2-Interval,
            {NState#state{toggle_next=NToggleNext}, max(Time, NToggleNext)}
    end;
config_change(Args, Time, #state{status=set, alarm_id=AlarmId}) ->
    alarm_handler:clear_alarm(AlarmId),
    init(Time, Args);
config_change(Args, Time, _) ->
    init(Time, Args).

%% @private
-spec terminate(Reason, State) -> ok when
      Reason :: any(),
      State :: #state{}.
terminate(_, #state{status=set, alarm_id=AlarmId}) ->
    alarm_handler:clear_alarm(AlarmId);
terminate(_, _) ->
    ok.

%% Internal

message_queue_status(QueueDelay, #state{target=Target})
  when QueueDelay < Target ->
    fast;
message_queue_status(0, #state{target=0}) ->
    case process_info(self(), message_queue_len) of
        {_, 0} ->
            fast;
        _ ->
            slow
    end;
message_queue_status(_, _) ->
    slow.
