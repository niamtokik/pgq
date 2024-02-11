%%%===================================================================
%%%
%%%===================================================================
-module(pgq_producer).
-behavior(gen_server).
-export([start/1, start_link/1]).
-export([publish/3]).
-export([init/1, terminate/2]).
-export([handle_cast/2, handle_call/3, handle_info/2]).
-include_lib("kernel/include/logger.hrl").

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
start(Args) ->
    gen_server:start(?MODULE, Args, []).

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
start_link(Args) ->
    gen_server:start(?MODULE, Args, []).

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
publish(Pid, Type, Content) ->
    gen_server:cast(Pid, {publish, {Type, Content}}).

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
init(#{ connection := _Connection, queue := _Queue} = Args) ->
    init_queue(Args);
init(#{ queue := _Queue } = Args) ->
    init_connection(Args).

% @hidden
init_connection(Args) ->
    case epgsql:connect(Args) of
        {ok, Connection} ->
            init_queue(Args#{ connection => Connection });
        Elsewise ->
            Elsewise
    end.

% @hidden
init_queue(Args) ->
    Connection = maps:get(connection, Args),
    Queue = maps:get(queue, Args),
    case pgq_low:get_queue_info(Connection, Queue) of
        {ok, []} ->
            pgq_low:create_queue(Connection, Queue),
            init_state(Args);
        {ok, _} ->
            init_state(Args)
    end.

% @hidden
init_state(Args) ->
    {ok, Args}.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
terminate(_,_) ->
    ok.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
handle_cast({publish, Events}, State) 
  when is_list(Events) ->
    Fun = fun (Event) -> publish_event(Event, State) end,
    lists:map(Fun, Events);
handle_cast({publish, {Type, Value}}, State) ->
    publish_event({Type, Value}, State),
    {noreply, State}.
    
%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
handle_call(_,_,State) ->
    {stop, undefined, State}.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
handle_info(_,State) ->
    {stop, undefined, State}.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
publish_event({Type, Value}, State) ->
    Connection = maps:get(connection, State),
    Queue = maps:get(queue, State),
    pgq_low:insert_event(Connection, Queue, Type, Value).
