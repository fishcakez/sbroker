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
%% @doc Sets an alarm when the process's message queue is slow for an interval.
%%
%% `sbroker_overload_meter' can be used as a `sbroker_meter' in a `sbroker' or
%% a `sregulator'. It will set a SASL `alarm_handler' alarm when the process's
%% message queue is slow for an interval and clear it once the message queue
%% becomes fast for an interval. Its argument, `spec()', is of the form:
%% ```
%% #{alarm    => Alarm :: any(), % default: {overload, self()}
%%   target   => Target :: non_neg_integer(), % default: 100
%%   interval => Interval :: pos_integer()}. % default: 1000
%% '''
%% `Alarm' is the `alarm_handler' alarm that will be set/cleared by the meter
%% (defaults to `{overload, self()}'). `Target' is the target sojourn time of
%% the message queue in milliseconds (defaults to `100'). `Interval' is the
%% interval in milliseconds (defaults to `1000') that the message queue must be
%% above the target for the alarm to be set and below the target for the alarm
%% to be cleared. The description of the alarm is
%% `{message_queue_slow, self()}'.
%%
%% This meter detects when the process is receiving more requests than it can
%% handle and not whether a `sbroker_queue' is congested. If a `sbroker_queue'
%% becomes congested it will drop requests to clear the congestion, causing a
%% congestion alarm (if one existed) to be cleared very quickly.
-module(sbroker_overload_meter).

-behaviour(sbroker_meter).

-export([init/2]).
-export([handle_update/5]).
-export([handle_info/3]).
-export([code_change/4]).
-export([config_change/3]).
-export([terminate/2]).

%% types

-type spec() ::
    #{alarm    => Alarm :: any(),
      target   => Target :: non_neg_integer(),
      interval => Interval :: pos_integer()}.

-export_type([spec/0]).

-record(state, {target :: non_neg_integer(),
                interval :: pos_integer(),
                alarm_id :: any(),
                status = clear :: clear | set,
                toggle_next = infinity :: integer() | infinity}).

%% @private
-spec init(Time, Spec) -> {State, infinity} when
      Time :: integer(),
      Spec :: spec(),
      State :: #state{}.
init(_, Spec) ->
    AlarmId = sbroker_util:alarm(overload, Spec),
    Target = sbroker_util:sojourn_target(Spec),
    Interval = sbroker_util:interval(Spec),
    alarm_handler:clear_alarm(AlarmId),
    {#state{target=Target, interval=Interval, alarm_id=AlarmId}, infinity}.

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
-spec config_change(Spec, Time, State) -> {NState, Next} when
      Spec :: spec(),
      Time :: integer(),
      State :: #state{},
      NState :: #state{},
      Next :: integer() | infinity.
config_change(Spec, Time,
              #state{alarm_id=AlarmId, interval=Interval, status=Status,
                     toggle_next=ToggleNext} = State) ->
    case sbroker_util:alarm(overload, Spec) of
        AlarmId when ToggleNext == infinity ->
            NTarget = sbroker_util:sojourn_target(Spec),
            NInterval = sbroker_util:interval(Spec),
            {State#state{target=NTarget, interval=NInterval}, infinity};
        AlarmId when is_integer(ToggleNext) ->
            NTarget = sbroker_util:sojourn_target(Spec),
            NInterval = sbroker_util:interval(Spec),
            NToggleNext = ToggleNext+NInterval-Interval,
            NState = State#state{target=NTarget, interval=NInterval,
                                 toggle_next=NToggleNext},
            {NState, max(Time, NToggleNext)};
        _ when Status == set ->
            alarm_handler:clear_alarm(AlarmId),
            init(Time, Spec);
        _ when Status == clear ->
            init(Time, Spec)
    end.

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
