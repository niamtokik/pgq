%%%===================================================================
%%%
%%%===================================================================
-module(pgq_consumer).
-behavior(gen_server).
% consumer record converter functions.
-export([to_tuple/1, to_record/1]).
% gen_server callback.
-export([init/1, terminate/2]).
-export([handle_call/3, handle_cast/2, handle_info/2]).
-include_lib("kernel/include/logger.hrl").
-include("pgq.hrl").

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
init(#{ connection := Connection
      , queue := _Queue
      , callback := Callback
      } = Args) ->
    pgq_low:ticker(Connection),
    {ok, Args}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
handle_call(_, _From, State) ->
    {reply, ok, State}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
handle_cast(_, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
handle_info(tick, #{connection := Connection, queue := Queue, callback := Callback} = State) ->
    handle_callback(State);
handle_info(_, State) ->
    {noreply, State}.

handle_callback(#{ connection := Connection
                 , queue := Queue
                 , consumer := Consumer
                 , callback := Callback
                 } = State) ->
    case pgq_low:next_batch(Connection, Queue, Consumer) of
        {ok, null} ->
            {noreply, State};
        {ok, BatchId} when is_integer(BatchId) ->
            handle_batch(BatchId, State)
    end.

handle_batch(BatchId, #{ connection := Connection
                       , queue := Queue
                       , consumer := Consumer
                       , callback := Callback
                       } = State) ->
    case pgq_low:get_batch_events(Connection, BatchId) of
        {ok, Events} ->
            ?LOG_INFO("~p", [Events]),
            handle_events(Events, BatchId, State);
        Elsewise ->
            {stop, Elsewise, State}
    end.

handle_events([], BatchId, #{ connection := Connection
                           , queue := Queue
                           , consumer := Consumer
                           , callback := Callback
                           } = State) ->
    ?LOG_INFO("finish batch ~p", [BatchId]),
    pgq_low:finish_batch(Connection, BatchId),
    {noreply, State};
handle_events([Event|Events], BatchId, #{ connection := Connection
                                        , queue := Queue
                                        , consumer := Consumer
                                        , callback := Callback
                                        } = State) ->
    ?LOG_INFO("handle event ~p in batch ~p with ~p", [Event, BatchId, Callback]),
    % {Module, Function, Arguments} = Callback,
    case Callback(Event) of
        ok ->
            handle_events(Events, BatchId, State);
        {ok, Result} ->
            handle_events(Events, BatchId, State);
        retry ->
            pgq_low:event_retry(Connection, BatchId, Event#pgq_event.id, 1)
    end.

terminate(_, _State) ->
    ok.

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
