%%%===================================================================
%%% @author Mathieu Kerjouan
%%% @copyright 2023 (c) Mathieu Kerjouan
%%% @doc
%%%
%%% This module contains functions to manage/monitor pgq tables.
%%%
%%% see: https://pgq.github.io/extension/pgq/files/schema-sql.html
%%% @end
%%%===================================================================
-module(pgq_tables).
-export([list_consumers/1, count_consumers/1, get_consumer/2]).
-export([list_queues/1, count_queues/1, get_queue/2]).
-export([list_ticks/1, count_ticks/1, get_tick/2]).
-export([list_subscriptions/1, count_subscriptions/1, get_subscription/2]).
-export([list_event_templates/1, count_event_templates/1, get_event_template/2]).
-export([list_retry_queues/1, count_retry_queues/1, get_retry_queue/2]).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
list_consumers(Connection) ->
    Query = "SELECT * FROM pgq.consumer;",
    QueryArgs = [],
    pgq_low:equery(Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
count_consumers(Connection) ->
    Query = "SELECT count(co_id) FROM pgq.consumer;",
    QueryArgs = [],
    pgq_low:equery(Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
get_consumer(Connection, ConsumerId) ->
    Query = "SELECT * FROM pgq.consumer WHERE co_id = $1::bigint;",
    QueryArgs = [ConsumerId],
    pgq_low:equery(Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
list_queues(Connection) ->
    Query = "SELECT * FROM pgq.queue;",
    QueryArgs = [],
    pgq_low:equery(Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
count_queues(Connection) ->
    Query = "SELECT count(queue_id) FROM pgq.queue;",
    QueryArgs = [],
    pgq_low:equery(Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
get_queue(Connection, QueueId) ->
    Query = "SELECT * FROM pgq.queue WHERE queue_id = $1::bigint;",
    QueryArgs = [QueueId],
    pgq_low:equery(Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
list_ticks(Connection) ->
    Query = "SELECT * FROM pgq.tick;",
    QueryArgs = [],
    pgq_low:equery(Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
count_ticks(Connection) ->
    Query = "SELECT count(tick_id) FROM pgq.tick;",
    QueryArgs = [],
    pgq_low:equery(Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
get_tick(Connection, TickId) ->
    Query = "SELECT * FROM pgq.tick WHERE tick_id = $1::bigint;",
    QueryArgs = [TickId],
    pgq_low:equery(Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
list_subscriptions(Connection) ->
    Query = "SELECT * FROM pgq.subscription;",
    QueryArgs = [],
    pgq_low:equery(Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
count_subscriptions(Connection) ->
    Query = "SELECT count(sub_id) FROM pgq.subscription;",
    QueryArgs = [],
    pgq_low:equery(Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
get_subscription(Connection, SubscriptionId) ->
    Query = "SELECT * FROM pgq.subscription WHERE subscription_id = $1::bigint;",
    QueryArgs = [SubscriptionId],
    pgq_low:equery(Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
list_event_templates(Connection) ->
    Query = "SELECT * FROM pgq.event_template;",
    QueryArgs = [],
    pgq_low:equery(Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
count_event_templates(Connection) ->
    Query = "SELECT count(ev_id) FROM pgq.event_template;",
    QueryArgs = [],
    pgq_low:equery(Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
get_event_template(Connection, Event_TemplateId) ->
    Query = "SELECT * FROM pgq.event_template WHERE event_template_id = $1::bigint;",
    QueryArgs = [Event_TemplateId],
    pgq_low:equery(Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
list_retry_queues(Connection) ->
    Query = "SELECT * FROM pgq.retry_queue;",
    QueryArgs = [],
    pgq_low:equery(Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
count_retry_queues(Connection) ->
    Query = "SELECT count(ev_id) FROM pgq.retry_queue;",
    QueryArgs = [],
    pgq_low:equery(Connection, Query, QueryArgs).

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
get_retry_queue(Connection, Retry_QueueId) ->
    Query = "SELECT * FROM pgq.retry_queue WHERE retry_queue_id = $1::bigint;",
    QueryArgs = [Retry_QueueId],
    pgq_low:equery(Connection, Query, QueryArgs).





    

