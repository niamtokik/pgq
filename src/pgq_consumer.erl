%%%===================================================================
%%%
%%%===================================================================
-module(pgq_consumer).
-export([to_tuple/1, to_record/1]).
-include("pgq.hrl").

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec to_tuple(Consumer) -> Return when
      Consumer :: #pgq_consumer{} | [#pgq_consumer{}],
      Return :: tuple() | [tuple()].

to_tuple(List) 
  when is_list(List) ->
    Fun = fun(Consumer) -> to_tuple(Consumer) end,
    lists:map(Fun, List);
to_tuple(#pgq_consumer{ queue_name = QueueName
                      , consumer_name = ConsumerName
                      , lag = Lag
                      , last_seen = LastSeen
                      , last_tick = LastTick
                      , current_batch = CurrentBatch
                      , next_tick = NextTick
                      , pending_events = PendingEvents
                      }) ->
    { QueueName, ConsumerName, Lag, LastSeen, LastTick, CurrentBatch
    , NextTick, PendingEvents
    }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec to_record(Consumer) -> Return when
      Consumer :: tuple() | [tuple()],
      Return :: #pgq_consumer{} | [#pgq_consumer{}].

to_record(List) 
  when is_list(List) ->
    Fun = fun(Consumer) -> to_record(Consumer) end,
    lists:map(Fun, List);
to_record({Consumer}) ->
    to_record(Consumer);
to_record({ QueueName, ConsumerName, Lag, LastSeen, LastTick, CurrentBatch
          , NextTick, PendingEvents }) ->
    #pgq_consumer{ queue_name = QueueName
                 , consumer_name = ConsumerName
                 , lag = Lag
                 , last_seen = LastSeen
                 , last_tick = LastTick
                 , current_batch = CurrentBatch
                 , next_tick = NextTick
                 , pending_events = PendingEvents
                 }.
