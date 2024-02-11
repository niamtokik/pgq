%%%===================================================================
%%% @author Mathieu Kerjouan
%%% @copyright 2023 (c) Mathieu Kerjouan
%%% @doc
%%%
%%% A simple producer interface using `gen_server' with pgq.
%%%
%%% @end
%%%===================================================================
-module(pgq_producer).
-behavior(gen_server).
-export([start/1, start_link/1]).
-export([publish/3, queue_info/1]).
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
    gen_server:call(Pid, {publish, {Type, Content}}).

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
queue_info(Pid) ->
    gen_server:call(Pid, {get, queue, info}).

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
init(#{ connection := _Connection, queue := _Queue} = Args) ->
    init_extension(Args);
init(#{ queue := _Queue } = Args) ->
    init_connection(Args).

% @hidden
init_connection(Args) ->
    case epgsql:connect(Args) of
        {ok, Connection} ->
            init_extension(Args#{ connection => Connection });
        Elsewise ->
            Elsewise
    end.

% @hidden
init_extension(#{ connection := Connection } = Args) ->
    case pgq_low:is_installed(Connection) of
        true -> 
            init_queue(Args);
        false -> {error, pgq_not_installed}
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
handle_call({get, queue, info}, _From, #{ queue := Queue } = State) ->
    Connection = maps:get(connection, State),
    Queue = maps:get(queue, State),
    case pgq_low:get_queue_info(Connection, Queue) of
        {ok, Info} -> 
            {reply, Info, State};
        Elsewise ->
            {reply, Elsewise, State}
    end;
handle_call({publish, {Type, Value}}, _From, State) ->
    try publish_event({Type, Value}, State) of
        Return -> 
            {reply, Return, State}
    catch
        E:R:_ ->
            {reply, {error, {E, R}}, State}
    end.
    
%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
handle_cast(_,State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
handle_info(_,State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%%
%%--------------------------------------------------------------------
publish_event({Type, Value}, State) ->
    Connection = maps:get(connection, State),
    Queue = maps:get(queue, State),
    pgq_low:insert_event(Connection, Queue, Type, Value).
