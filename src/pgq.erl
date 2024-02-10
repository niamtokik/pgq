%%%===================================================================
%%% @doc
%%% @end
%%%===================================================================
-module(pgq).
-export([create_queue/2, drop_queue/2, drop_queue/3, set_queue_config/4]).
-export([insert_event/4, insert_event/5, current_event_table/2]).
-export([register_consumer/3, register_consumer_at/4, unregister_consumer/3]).
-export([next_batch_info/3, next_batch/3, get_batch_event/2, event_retry/4]).
-export([batch_retry/3, finish_batch/2]).
-export([get_queue_info/1, get_queue_info/2]).
-export([get_consumer_info/1, get_consumer_info/2, get_consumer_info/3]).
-export([version/1]).
-export([get_batch_info/2, batch_event_sql/2, batch_event_tables/2]).
-export([ticker/1, ticker/2]).
-export([maint_retry_events/1]).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.create_queue(1)
%% @end
%%--------------------------------------------------------------------
create_queue(Connection, QueueName) ->
    epgsql:equery(Connection, "SELECT pgq.create_queue($1::text);"
                            , [QueueName]).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.drop_queue(1)
%% @end
%%--------------------------------------------------------------------
drop_queue(Connection, QueueName) ->
    epgsql:equery(Connection, "SELECT pgq.drop_queue($1::text);"
                            , [QueueName]).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.drop_queue(2)
%% @end
%%--------------------------------------------------------------------
drop_queue(Connection, QueueName, Force) ->
    epgsql:equery(Connection, "SELECT pgq.drop_queue($1::text, $2::boolean);"
                            , [QueueName, Force]).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.set_queue_config(3)
%% @end
%%--------------------------------------------------------------------
set_queue_config(Connection, QueueName, ParamName, ParamValue) ->
    epgsql:equery(Connection, "SELECT pgq.set_queue_config($1::text, $2::text, $3::text);"
                            , [QueueName, ParamName, ParamValue]).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.insert_event(3)
%% @end
%%--------------------------------------------------------------------
insert_event(Connection, QueueName, EventType, EventData) ->
    epgsql:equery(Connection, "SELECT pgq.insert_event($1::text, $2::text, $3::text);"
                            , [QueueName, EventType, EventData]).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.insert_event(7)
%% @end
%%--------------------------------------------------------------------
insert_event(Connection, QueueName, EventType, EventData, Extra) ->
    Extra1 = maps:get(Extra, extra1, null),
    Extra2 = maps:get(Extra, extra2, null),
    Extra3 = maps:get(Extra, extra3, null),
    Extra4 = maps:get(Extra, extra4, null),
    epgsql:equery(Connection, "SELECT pgq.insert_event($1::text, $2::text, $3::text, $4::text, $5::text, $6::text, $7::text);"
                            , [QueueName, EventType, EventData, Extra1, Extra2, Extra3, Extra4]).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.current_event_table(1)
%% @end
%%--------------------------------------------------------------------
current_event_table(Connection, QueueName) ->
    epgsql:equery(Connection, "SELECT pgq.current_event_table($1::text);"
                            , [QueueName]).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.register_consumer(2)
%% @end
%%--------------------------------------------------------------------
register_consumer(Connection, QueueName, ConsumerId) ->
    epgsql:equery(Connection, "SELECT pgq.register_consumer($1::text, $2::text);"
                            , [QueueName, ConsumerId]).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.register_consumer_at(3)
%% @end
%%--------------------------------------------------------------------
register_consumer_at(Connection, QueueName, ConsumerName, TickPos) ->
    epgsql:equery(Connection, "SELECT pgq.register_consumer($1::text, $2::text, $3::bigint);"
                            , [QueueName, ConsumerName, TickPos]).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.unregister_consumer(2)
%% @end
%%--------------------------------------------------------------------
unregister_consumer(Connection, QueueName, ConsumerName) ->
    epgsql:equery(Connection, "SELECT pgq.unregister_consumer($1::text, $2::text);"
                            , [QueueName, ConsumerName]).


%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.next_batch_info(2)
%% @end
%%--------------------------------------------------------------------
next_batch_info(Connection, QueueName, ConsumerName) ->
    epgsql:equery(Connection, "SELECT pgq.next_batch_info($1::text, $2::text);"
                            , [QueueName, ConsumerName]).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.next_batch(2)
%% @end
%%--------------------------------------------------------------------
next_batch(Connection, QueueName, ConsumerName) ->
    epgsql:equery(Connection, "SELECT pgq.next_batch($1::text, $2::text);"
                            , [QueueName, ConsumerName]).

%%--------------------------------------------------------------------
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.next_batch_custom(5)
%%--------------------------------------------------------------------
% next_batch(Connection, QueueName, ConsumerName, _Opts) ->
%     epgsql:equery(Connection, "SELECT pgq.next_batch($1::text, $2::text);"
%                             , [QueueName, ConsumerName]).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.get_batch_events(1)
%% @end
%%--------------------------------------------------------------------
get_batch_event(Connection, BatchId) ->
    epgsql:equery(Connection, "SELECT pgq.get_batch_event($1::bigint);"
                            , [BatchId]).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.event_retry(3a)
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.event_retry(3b)
%% @end
%%--------------------------------------------------------------------
event_retry(Connection, BatchId, EventId, Timestamp)
  when is_tuple(Timestamp) ->
    epgsql:equery(Connection, "SELECT pgq.event_retry($1::bigint, $2::text, $3::timestamptz);"
                            , [BatchId, EventId, Timestamp]);
event_retry(Connection, BatchId, EventId, Seconds)
  when is_integer(Seconds) ->
    epgsql:equery(Connection, "SELECT pgq.event_retry($1::bigint, $2::text, $3::integer);"
                            , [BatchId, EventId, Seconds]).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.batch_retry(2)
%% @end
%%--------------------------------------------------------------------
batch_retry(Connection, BatchId, RetrySeconds) ->
    epgsql:equery(Connection, "SELECT pgq.batch_retry($1::bigint, $3::integer);"
                            , [BatchId, RetrySeconds]).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.finish_batch(1)
%% @end
%%--------------------------------------------------------------------
finish_batch(Connection, BatchId) ->
    epgsql:equery(Connection, "SELECT pgq.finish_batch($1::bigint);"
                            , [BatchId]).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.get_queue_info(0)
%% @end
%%--------------------------------------------------------------------
get_queue_info(Connection) ->
    epgsql:equery(Connection, "SELECT pgq.get_queue_info();", []).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.get_queue_info(1)
%% @end
%%--------------------------------------------------------------------
get_queue_info(Connection, QueueName) ->
    epgsql:equery(Connection, "SELECT pgq.get_queue_info($1::text);"
                            , [QueueName]).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.get_consumer_info(0)
%% @end
%%--------------------------------------------------------------------
get_consumer_info(Connection) ->
    epgsql:equery(Connection, "SELECT pgq.get_consumer_info();", []).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.get_consumer_info(1)
%% @end
%%--------------------------------------------------------------------
get_consumer_info(Connection, QueueName) ->
    epgsql:equery(Connection, "SELECT pgq.get_consumer_info($1::text);"
                            , [QueueName]).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.get_consumer_info(2)
%% @end
%%--------------------------------------------------------------------
get_consumer_info(Connection, QueueName, ConsumerName) ->
    epgsql:equery(Connection, "SELECT pgq.get_consumer_info($1::text, $2::text);"
                            , [QueueName, ConsumerName]).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.version(0)
%% @end
%%--------------------------------------------------------------------
version(Connection) ->
    epgsql:equery(Connection, "SELECT version();", []).


%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.get_batch_info(1)
%% @end
%%--------------------------------------------------------------------
get_batch_info(Connection, BatchId) ->
    epgsql:equery(Connection, "SELECT pgq.get_batch_info($1::bigint);"
                            , [BatchId]).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/internal-sql.html#pgq.batch_event_sql(1)
%% @end
%%--------------------------------------------------------------------
batch_event_sql(Connection, BatchId) ->
    epgsql:equery(Connection, "SELECT pgq.batch_event_sql($1::bigint);"
                            , [BatchId]).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/internal-sql.html#pgq.batch_event_tables(1)
%% @end
%%--------------------------------------------------------------------
batch_event_tables(Connection, BatchId) ->
    epgsql:equery(Connection, "SELECT pgq.batch_event_tables($1::bigint);"
                            , [BatchId]).

%%--------------------------------------------------------------------
%% https://pgq.github.io/extension/pgq/files/internal-sql.html#pgq.event_retry_raw(12)
%%--------------------------------------------------------------------
% event_retry_raw(Connection, QueueName, ConsumerName, Opts) ->
%     epgsql:equery(Connection, "SELECT pgq.event_retry_raw($1::bigint);"
%                             , [BatchId]).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/internal-sql.html#pgq.ticker(0)
%% @end
%%--------------------------------------------------------------------
ticker(Connection) ->
    epgsql:equery(Connection, "SELECT pgq.ticker();", []).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/internal-sql.html#pgq.ticker(1)
%% @end
%%--------------------------------------------------------------------
ticker(Connection, QueueName) ->
    epgsql:equery(Connection, "SELECT pgq.ticker($1::text);"
                            , [QueueName]).

%%--------------------------------------------------------------------
%% https://pgq.github.io/extension/pgq/files/internal-sql.html#pgq.ticker(3)
%%--------------------------------------------------------------------
% ticker(Connection, QueueName, TickId, Timestamp, EventSeq) ->
%     epgsql:equery(Connection, "SELECT pgq.ticker($1::text);"
%                             , [QueueName]).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/internal-sql.html#pgq.maint_retry_events(0)
%% @end
%%--------------------------------------------------------------------
maint_retry_events(Connection) ->
    epgsql:equery(Connection, "SELECT pgq.maint_retry_events();", []).

%%--------------------------------------------------------------------
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
maint_rotate_tables_step1(Connection, QueueName) ->
    epgsql:equery(Connection, "SELECT pgq.maint_rotate_tables_step1($1::text);"
                            , [QueueName]).

%%--------------------------------------------------------------------
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
maint_rotate_tables_step2(Connection) ->
    epgsql:equery(Connection, "SELECT pgq.maint_rotate_tables_step2();", []).

%%--------------------------------------------------------------------
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
maint_tables_to_vacuum(Connection) ->
    epgsql:equery(Connection, "SELECT pgq.maint_tables_to_vacuum();", []).

%%--------------------------------------------------------------------
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
maint_operations(Connection) ->
    epgsql:equery(Connection, "SELECT pgq.maint_operations();", []).

%%--------------------------------------------------------------------
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
grant_perms(Connection, QueueName) ->
    epgsql:equery(Connection, "SELECT pgq.maint_operations($1::text);"
                            , [QueueName]).
    
%%--------------------------------------------------------------------
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
tune_storage(Connection, QueueName) ->
    epgsql:equery(Connection, "SELECT pgq.tune_storage($1::text);"
                            , [QueueName]).

%%--------------------------------------------------------------------
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
force_tick(Connection, QueueName) ->
    epgsql:equery(Connection, "SELECT pgq.force_tick($1::text);"
                            , [QueueName]).
    
%%--------------------------------------------------------------------
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
seq_getval(Connection, SeqName) ->
    epgsql:equery(Connection, "SELECT pgq.seq_getval($1::text);"
                            , [SeqName]).

%%--------------------------------------------------------------------
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
seq_getval(Connection, SeqName, NewValue) ->
    epgsql:equery(Connection, "SELECT pgq.seq_getval($1::text; $2::int8);"
                            , [SeqName, NewValue]).
    
%%--------------------------------------------------------------------
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
quote_fqname(Connection, Name) ->
    epgsql:equery(Connection, "SELECT pgq.quote_fqname($1::text);"
                            , [Name]).
    