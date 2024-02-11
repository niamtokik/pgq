%%--------------------------------------------------------------------
%% A queue definition.
%%--------------------------------------------------------------------
-record(pgq_queue, { name :: binary()
                   , ntables :: integer()
                   , cur_table :: integer()
                   , rotation_period :: tuple()
                   , switch_time :: tuple()
                   , external_ticker :: boolean()
                   , ticker_paused :: boolean()
                   , ticker_max_count :: integer()
                   , ticker_max_lag :: tuple()
                   , ticker_idle_period :: tuple()
                   , ticker_lag :: tuple()
                   , ev_per_sec :: float()
                   , ev_new :: integer()
                   , last_tick_id :: integer()
                   }).

%%--------------------------------------------------------------------
%% A consumer definition
%%--------------------------------------------------------------------
-record(pgq_consumer, { queue_name :: binary()
                      , consumer_name :: binary()
                      , lag :: tuple()
                      , last_seen :: tuple()
                      , last_tick :: integer()
                      , current_batch :: integer()
                      , next_tick :: integer()
                      , pending_events :: integer()
                      }).

%%--------------------------------------------------------------------
%% An event definition.
%%--------------------------------------------------------------------
-record(pgq_event, { id :: integer()
                   , time :: tuple()
                   , txid :: integer()
                   , retry :: integer()
                   , type :: binary()
                   , data :: binary()
                   , extra1 :: binary()
                   , extra2 :: binary()
                   , extra3 :: binary()
                   , extra4 :: binary()
                   }).
