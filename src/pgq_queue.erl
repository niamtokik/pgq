%%%===================================================================
%%% @doc
%%% @end
%%%===================================================================
-module(pgq_queue).
-export([to_tuple/1, to_record/1]).
-include("pgq.hrl").

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec to_tuple(Queue) -> Return when
      Queue :: #pgq_queue{} | [#pgq_queue{}],
      Return :: tuple() | [tuple()].

to_tuple(List) 
  when is_list(List) ->
    Fun = fun(Queue) -> to_tuple(Queue) end,
    lists:map(Fun, List);
to_tuple(#pgq_queue{ name = Name
                   , ntables = Ntables
                   , cur_table = CurTable
                   , rotation_period = RotationPeriod
                   , switch_time = SwitchTime
                   , external_ticker = ExternalTicker
                   , ticker_paused = TickerPaused
                   , ticker_max_count = TickerMaxCount
                   , ticker_max_lag = TickerMaxLag
                   , ticker_idle_period = TickerIdlePeriod
                   , ticker_lag = TickerLag
                   , ev_per_sec = EvPerSec
                   , ev_new = EvNew
                   , last_tick_id = LastTickId
                   }) ->
    { Name, Ntables, CurTable, RotationPeriod, SwitchTime, ExternalTicker
    , TickerPaused, TickerMaxCount, TickerMaxLag, TickerIdlePeriod
    , TickerLag, EvPerSec, EvNew, LastTickId
    }.

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec to_record(Queue) -> Return when
      Queue :: tuple() | [tuple()],
      Return :: #pgq_queue{} | [#pgq_queue{}].

to_record(List) 
  when is_list(List) ->
    Fun = fun(Queue) -> to_record(Queue) end,
    lists:map(Fun, List);
to_record({Queue}) ->
    to_record(Queue);
to_record({ Name, Ntables, CurTable, RotationPeriod, SwitchTime, ExternalTicker
          , TickerPaused, TickerMaxCount, TickerMaxLag, TickerIdlePeriod
          , TickerLag, EvPerSec, EvNew, LastTickId }) ->
    #pgq_queue{ name = Name
              , ntables = Ntables
              , cur_table = CurTable
              , rotation_period = RotationPeriod
              , switch_time = SwitchTime
              , external_ticker = ExternalTicker
              , ticker_paused = TickerPaused
              , ticker_max_count = TickerMaxCount
              , ticker_max_lag = TickerMaxLag
              , ticker_idle_period = TickerIdlePeriod
              , ticker_lag = TickerLag
              , ev_per_sec = EvPerSec
              , ev_new = EvNew
              , last_tick_id = LastTickId
              }.
