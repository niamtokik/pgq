%%%===================================================================
%%% @author Mathieu Kerjouan
%%% @copyright 2023 (c) Mathieu Kerjouan
%%% @doc
%%%
%%% This module is a raw implement of the low level interface to pgq
%%% using SQL query only.
%%%
%%% The whole documentation is available here:
%%% https://pgq.github.io/extension/pgq/files/external-sql.html
%%% 
%%% @end
%%%===================================================================
-module(pgq_low).
-export([is_installed/1, version/1, version_all/1]).
% queue management
-export([create_queue/2, drop_queue/2, drop_queue/3, set_queue_config/4]).
-export([get_queue_info/1, get_queue_info/2]).
% event management
-export([insert_event/4, insert_event/5, current_event_table/2, event_retry/4]).
-export([maint_retry_events/1, event_retry_raw/5]).
% consumer management
-export([register_consumer/3, register_consumer_at/4, unregister_consumer/3]).
-export([get_consumer_info/1, get_consumer_info/2, get_consumer_info/3]).
% batch management
-export([next_batch_info/3, next_batch/3, next_batch/4, get_batch_event/2]).
-export([batch_retry/3, finish_batch/2]).
-export([get_batch_info/2, batch_event_sql/2, batch_event_tables/2]).
% tick management
-export([ticker/1, ticker/2, ticker/5]).
% maintenance
-export([maint_rotate_tables_step1/2, maint_rotate_tables_step2/1]).
-export([maint_tables_to_vacuum/1, maint_operations/1]).
-export([grant_perms/2]).
-export([tune_storage/2, force_tick/2, seq_getval/2, seq_getval/3]).
-export([quote_fqname/2]).
% trigger management
% -export([json_trigger/2, logu_trigger/2, sql_trigger/2]).
-include_lib("kernel/include/logger.hrl").

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
-type connection() :: epgsql:connection().
-type queue_name() :: binary() | string().
-type consumer_name() :: binary() | string().
-type event_type() :: binary() | string().
-type event_data() :: binary() | string().

%%--------------------------------------------------------------------
%% @hidden
%%--------------------------------------------------------------------
equery(Connection, Query, QueryArgs) ->
    QueryId = erlang:tunique_integer(),
    Apply = [Connection, Query, QueryArgs],
    Return = apply(epgsql, equery, Apply),
    ?LOG_DEBUG("~p", [#{ query_id => QueryId 
                       , module => ?MODULE
                       , file => ?FILE
                       , line => ?LINE
                       , function => ?FUNCTION_NAME
                       , arity => ?FUNCTION_ARITY
                       , call => {epgsql, equery, Apply}
                       , return => Return
                       }]),
    Return.

%%--------------------------------------------------------------------
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
-spec is_installed(Connection) -> Return when
      Connection :: connection(),
      Return :: boolean().

is_installed(Connection) ->
    case version(Connection) of
        {ok, _} -> true;
        _ -> false
    end.

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.version(0)
%% @end
%%--------------------------------------------------------------------
-spec version(Connection) -> Return when
      Connection :: connection(),
      Return :: {ok, binary()}
              | {error, term()}.

version(Connection) ->
    Query = "SELECT pgq.version();",
    QueryArgs = [],
    case equery(Connection, Query, QueryArgs) of
        {ok, _, [{Version}]} ->
            {ok, Version};
        Elsewise ->
            Elsewise
    end.

%%--------------------------------------------------------------------
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
version_all(Connection) ->
    Query = "SELECT * FROM pg_extension WHERE extname = $1;",
    QueryArgs = ["pgq"],
    equery(Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.create_queue(1)
%% @end
%%--------------------------------------------------------------------
-spec create_queue(Connection, QueueName) -> Return when
      Connection :: connection(),
      QueueName :: queue_name(),
      Return :: {ok, integer()}
              | {error, term()}.

create_queue(Connection, QueueName) ->
    Query = "SELECT pgq.create_queue($1::text);",
    QueryArgs = [QueueName],
    Return = equery(Connection, Query, QueryArgs),
    case Return of
        {ok, _, [{Counter}]} ->
            {ok, Counter};
        Elsewise ->
            Elsewise
    end.

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.drop_queue(1)
%% @end
%%--------------------------------------------------------------------
-spec drop_queue(Connection, QueueName) -> Return when
      Connection :: connection(),
      QueueName :: queue_name(),
      Return :: {ok, integer()}
              | {error, term()}.

drop_queue(Connection, QueueName) ->
    Query = "SELECT pgq.drop_queue($1::text);",
    QueryArgs = [QueueName],
    Return= equery(Connection, Query, QueryArgs),
    case Return of
        {ok, _, [{Counter}]} ->
            {ok, Counter};
        Elsewise ->
            Elsewise
    end.

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.drop_queue(2)
%% @end
%%--------------------------------------------------------------------
-spec drop_queue(Connection, QueueName, Force) -> Return when
      Connection :: connection(),
      QueueName :: queue_name(),
      Force :: boolean(),
      Return :: {ok, integer()}
              | {error, term()}.

drop_queue(Connection, QueueName, Force) ->
    Query = "SELECT pgq.drop_queue($1::text, $2::boolean);",
    QueryArgs = [QueueName, Force],
    Return = equery(Connection, Query, QueryArgs),
    case Return of
        {ok, _, [{Counter}]} ->
            {ok, Counter};
        Elsewise ->
            Elsewise
    end.    

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.set_queue_config(3)
%% @end
%%--------------------------------------------------------------------
-spec set_queue_config(Connection, QueueName, ParamName, ParamValue) -> Return when
      Connection :: connection(),
      QueueName :: queue_name(),
      ParamName :: binary() | string(),
      ParamValue :: binary() | string(),
      Return :: any().

set_queue_config(Connection, QueueName, ParamName, ParamValue) ->
    Query = "SELECT pgq.set_queue_config($1::text, $2::text, $3::text);",
    QueryArgs = [QueueName, ParamName, ParamValue],
    equery(Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.insert_event(3)
%% @end
%%--------------------------------------------------------------------
-spec insert_event(Connection, QueueName, EventType, EventData) -> Return when
      Connection :: connection(),
      QueueName :: queue_name(),
      EventType :: event_type(),
      EventData :: event_data(),
      Return :: {ok, integer()}
              | {error, term()}.

insert_event(Connection, QueueName, EventType, EventData) ->
    Query = "SELECT pgq.insert_event($1::text, $2::text, $3::text);",
    QueryArgs = [QueueName, EventType, EventData],
    Return = equery(Connection, Query, QueryArgs),
    case Return of
        {ok, _, [{Events}]} ->
            {ok, Events};
        Elsewise ->
            Elsewise
    end.

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.insert_event(7)
%% @end
%%--------------------------------------------------------------------
-spec insert_event(Connection, QueueName, EventType, EventData, Extra) -> Return when
      Connection :: connection(),
      QueueName :: queue_name(),
      EventType :: event_type(),
      EventData :: event_data(),
      Extra :: #{ extra1 => event_data() | null
                , extra2 => event_data() | null
                , extra3 => event_data() | null
                , extra4 => event_data() | null
                },
      Return :: {ok, integer()}
              | {error, term()}.

insert_event(Connection, QueueName, EventType, EventData, Extra) ->
    Extra1 = maps:get(Extra, extra1, null),
    Extra2 = maps:get(Extra, extra2, null),
    Extra3 = maps:get(Extra, extra3, null),
    Extra4 = maps:get(Extra, extra4, null),
    Query = "SELECT pgq.insert_event($1::text, $2::text, $3::text, "
            "$4::text, $5::text, $6::text, $7::text);",
    QueryArgs = [QueueName, EventType, EventData
                ,Extra1, Extra2, Extra3, Extra4],
    Return = equery(Connection, Query, QueryArgs),
    case Return of
        {ok, _, [{Events}]} ->
            {ok, Events};
        Elsewise ->
            Elsewise
    end.

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.current_event_table(1)
%% @end
%%--------------------------------------------------------------------
current_event_table(Connection, QueueName) ->
    Query = "SELECT pgq.current_event_table($1::text);",
    QueryArgs = [QueueName],
    equery(Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.register_consumer(2)
%% @end
%%--------------------------------------------------------------------
-spec register_consumer(Connection, QueueName, ConsumerId) -> Return when
      Connection :: connection(),
      QueueName :: queue_name(),
      ConsumerId :: consumer_name(),
      Return :: {ok, integer()}
              | {error, term()}.

register_consumer(Connection, QueueName, ConsumerId) ->
    Query = "SELECT pgq.register_consumer($1::text, $2::text);",
    QueryArgs = [QueueName, ConsumerId],
    Return = equery(Connection, Query, QueryArgs),
    case Return of
        {ok, _, [{Counter}]} ->
            {ok, Counter};
        Elsewise ->
            Elsewise
    end.
            

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.register_consumer_at(3)
%% @end
%%--------------------------------------------------------------------
register_consumer_at(Connection, QueueName, ConsumerName, TickPos) ->
    Query = "SELECT pgq.register_consumer($1::text, $2::text, $3::bigint);",
    QueryArgs = [QueueName, ConsumerName, TickPos],
    _Return = equery(Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.unregister_consumer(2)
%% @end
%%--------------------------------------------------------------------
-spec unregister_consumer(Connection, QueueName, ConsumerName) -> Return when
      Connection :: connection(),
      QueueName :: queue_name(),
      ConsumerName :: consumer_name(),
      Return :: {ok, integer()}
              | {error, term()}.

unregister_consumer(Connection, QueueName, ConsumerName) ->
    Query = "SELECT pgq.unregister_consumer($1::text, $2::text);",
    QueryArgs = [QueueName, ConsumerName],
    _Return = equery(Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.next_batch_info(2)
%% @end
%%--------------------------------------------------------------------
next_batch_info(Connection, QueueName, ConsumerName) ->
    Query = "SELECT pgq.next_batch_info($1::text, $2::text);",
    QueryArgs = [QueueName, ConsumerName],
    _Return = equery(Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.next_batch(2)
%% @end
%%--------------------------------------------------------------------
next_batch(Connection, QueueName, ConsumerName) ->
    Query = "SELECT pgq.next_batch($1::text, $2::text);",
    QueryArgs = [QueueName, ConsumerName],
    _Return = equery(Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.next_batch_custom(5)
%%--------------------------------------------------------------------
next_batch(Connection, QueueName, ConsumerName, Opts) ->
    MinLag = maps:get(min_lag, Opts, null),
    MinCount = maps:get(min_count, Opts, null),
    MinInterval = maps:get(min_interval, Opts, null),
    Query = "SELECT pgq.next_batch($1::text, $2::text, $3::interval, $4::integer, $5::interval);",
    QueryArgs = [QueueName, ConsumerName, MinLag, MinCount, MinInterval],
    _Return = equery(Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.get_batch_events(1)
%% @end
%%--------------------------------------------------------------------
get_batch_event(Connection, BatchId) ->
    Query = "SELECT pgq.get_batch_event($1::bigint);",
    QueryArgs = [BatchId],
    _Return = equery( Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.event_retry(3a)
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.event_retry(3b)
%% @end
%%--------------------------------------------------------------------
event_retry(Connection, BatchId, EventId, Timestamp)
  when is_tuple(Timestamp) ->
    Query = "SELECT pgq.event_retry($1::bigint, $2::text, $3::timestamptz);",
    QueryArgs = [BatchId, EventId, Timestamp],
    _Return = equery(Connection, Query, QueryArgs);
event_retry(Connection, BatchId, EventId, Seconds)
  when is_integer(Seconds) ->
    Query = "SELECT pgq.event_retry($1::bigint, $2::text, $3::integer);",
    QueryArgs = [BatchId, EventId, Seconds],
    _Return = equery( Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.batch_retry(2)
%% @end
%%--------------------------------------------------------------------
batch_retry(Connection, BatchId, RetrySeconds) ->
    Query = "SELECT pgq.batch_retry($1::bigint, $3::integer);",
    QueryArgs = [BatchId, RetrySeconds],
    _Return = equery(Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.finish_batch(1)
%% @end
%%--------------------------------------------------------------------
finish_batch(Connection, BatchId) ->
    Query = "SELECT pgq.finish_batch($1::bigint);",
    QueryArgs = [BatchId],
    _Return = equery(Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.get_queue_info(0)
%% @end
%%--------------------------------------------------------------------
get_queue_info(Connection) ->
    Query = "SELECT pgq.get_queue_info();",
    QueryArgs = [],
    Return = equery(Connection, Query, QueryArgs),
    case Return of
        {ok, _, Queues} ->
            {ok, pgq_queue:to_record(Queues)};
        Elsewise -> 
            Elsewise
    end.

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.get_queue_info(1)
%% @end
%%--------------------------------------------------------------------
get_queue_info(Connection, QueueName) ->
    Query = "SELECT pgq.get_queue_info($1::text);",
    QueryArgs = [QueueName],
    Return = equery(Connection, Query, QueryArgs),
    case Return of
        {ok, _, Queue} ->
            {ok, pgq_queue:to_record(Queue)};
        Elsewise -> 
            Elsewise
    end.

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.get_consumer_info(0)
%% @end
%%--------------------------------------------------------------------
get_consumer_info(Connection) ->
    Query = "SELECT pgq.get_consumer_info();",
    QueryArgs = [],
    Return = equery( Connection, Query, QueryArgs),
    case Return of
        {ok, _, Consumers} ->
            {ok, pgq_consumer:to_record(Consumers)};
        Elsewise ->
            Elsewise
    end.

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.get_consumer_info(1)
%% @end
%%--------------------------------------------------------------------
get_consumer_info(Connection, QueueName) ->
    Query = "SELECT pgq.get_consumer_info($1::text);",
    QueryArgs = [QueueName],
    Return = equery( Connection, Query, QueryArgs),
    case Return of
        {ok, _, Consumers} ->
            {ok, pgq_consumer:to_record(Consumers)};
        Elsewise ->
            Elsewise
    end.

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.get_consumer_info(2)
%% @end
%%--------------------------------------------------------------------
get_consumer_info(Connection, QueueName, ConsumerName) ->
    Query = "SELECT pgq.get_consumer_info($1::text, $2::text);",
    QueryArgs = [QueueName, ConsumerName],
    Return = equery(Connection, Query, QueryArgs),
    case Return of
        {ok, _, [Consumer]} ->
            {ok, pgq_consumer:to_record(Consumer)};
        Elsewise ->
            Elsewise
    end.    

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/external-sql.html#pgq.get_batch_info(1)
%% @end
%%--------------------------------------------------------------------
get_batch_info(Connection, BatchId) ->
    Query = "SELECT pgq.get_batch_info($1::bigint);",
    QueryArgs = [BatchId],
    _Return = equery(Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/internal-sql.html#pgq.batch_event_sql(1)
%% @end
%%--------------------------------------------------------------------
batch_event_sql(Connection, BatchId) ->
    Query = "SELECT pgq.batch_event_sql($1::bigint);",
    QueryArgs = [BatchId],
    _Return = equery(Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/internal-sql.html#pgq.batch_event_tables(1)
%% @end
%%--------------------------------------------------------------------
batch_event_tables(Connection, BatchId) ->
    Query = "SELECT pgq.batch_event_tables($1::bigint);",
    QueryArgs = [BatchId],
    _Return = equery( Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/internal-sql.html#pgq.event_retry_raw(12)
%% @end
%%--------------------------------------------------------------------
event_retry_raw(Connection, QueueName, ConsumerName, EventId, Opts) ->
    RetryAfter = maps:get(retry_after, Opts, null),
    EventTime = maps:get(event_time, Opts, null),
    EventRetry = maps:get(event_retry, Opts, null),
    EventType = maps:get(event_type, Opts, null),
    EventData = maps:get(event_data, Opts, null),
    Extra1 = maps:get(extra1, Opts, null),
    Extra2 = maps:get(extra2, Opts, null),
    Extra3 = maps:get(extra3, Opts, null),
    Extra4 = maps:get(extra4, Opts, null),
    Query = "SELECT pgq.event_retry_raw($1::text, $2::text, "
            "$3::timestamptz, $4::bigint, $5::timestamptz, "
            "$6::integer, $7::text, $8::text, "
            "$9::text, $10::text, $11::text, $12::text);",
    QueryArgs = [QueueName, ConsumerName, RetryAfter, EventId
                ,EventTime, EventRetry, EventType, EventData
                ,Extra1, Extra2, Extra3, Extra4],
    _Return = equery(Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/internal-sql.html#pgq.ticker(0)
%% @end
%%--------------------------------------------------------------------
ticker(Connection) ->
    Query = "SELECT pgq.ticker();",
    QueryArgs = [],
    _Return = equery(Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/internal-sql.html#pgq.ticker(1)
%% @end
%%--------------------------------------------------------------------
ticker(Connection, QueueName) ->
    Query = "SELECT pgq.ticker($1::text);",
    QueryArgs = [QueueName],
    _Return = equery(Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% https://pgq.github.io/extension/pgq/files/internal-sql.html#pgq.ticker(3)
%%--------------------------------------------------------------------
ticker(Connection, QueueName, TickId, Timestamp, EventSeq) ->
    Query = "SELECT pgq.ticker($1::text, $2::bigint, $3::timestamptz, $4::bigint);",
    QueryArgs = [QueueName, TickId, Timestamp, EventSeq],
    _Return = equery(Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%% https://pgq.github.io/extension/pgq/files/internal-sql.html#pgq.maint_retry_events(0)
%% @end
%%--------------------------------------------------------------------
maint_retry_events(Connection) ->
    Query = "SELECT pgq.maint_retry_events();",
    QueryArgs = [],
    _Return = equery(Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
maint_rotate_tables_step1(Connection, QueueName) ->
    Query = "SELECT pgq.maint_rotate_tables_step1($1::text);",
    QueryArgs = [QueueName],
    _Return = equery(Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
maint_rotate_tables_step2(Connection) ->
    Query = "SELECT pgq.maint_rotate_tables_step2();",
    QueryArgs = [],
    _Return = equery(Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
maint_tables_to_vacuum(Connection) ->
    Query = "SELECT pgq.maint_tables_to_vacuum();",
    QueryArgs = [],
    _Return = equery(Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
maint_operations(Connection) ->
    Query = "SELECT pgq.maint_operations();",
    QueryArgs = [],
    _Return = equery(Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
grant_perms(Connection, QueueName) ->
    Query = "SELECT pgq.maint_operations($1::text);",
    QueryArgs = [QueueName],    
    _Return = equery(Connection, Query, QueryArgs).
    
%%--------------------------------------------------------------------
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
tune_storage(Connection, QueueName) ->
    Query = "SELECT pgq.tune_storage($1::text);",
    QueryArgs = [QueueName],
    _Return = equery(Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
force_tick(Connection, QueueName) ->
    Query = "SELECT pgq.force_tick($1::text);",
    QueryArgs = [QueueName],
    _Return = equery(Connection, Query, QueryArgs).
    
%%--------------------------------------------------------------------
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
seq_getval(Connection, SeqName) ->
    Query = "SELECT pgq.seq_getval($1::text);",
    QueryArgs = [SeqName],
    _Return = equery( Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
seq_getval(Connection, SeqName, NewValue) ->
    Query = "SELECT pgq.seq_getval($1::text; $2::int8);",
    QueryArgs = [SeqName, NewValue],
    _Return = equery(Connection, Query, QueryArgs).
    
%%--------------------------------------------------------------------
%% @doc
%%
%% @end
%%--------------------------------------------------------------------
quote_fqname(Connection, Name) ->
    Query = "SELECT pgq.quote_fqname($1::text);",
    QueryArgs = [Name],
    _Return = equery(Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% trigger
%%--------------------------------------------------------------------
% json_trigger(Connection, TriggerName, QueueName, Args) ->
%     epgsql:equery( Connection
%                  , "CREATE TRIGGER triga_nimi AFTER INSERT OR UPDATE ON customer"
%                    "FOR EACH ROW EXECUTE PROCEDURE pgq.jsontriga('qname');"
%                  , []).
