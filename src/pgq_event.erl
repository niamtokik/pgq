-module(pgq_event).
-export([to_record/1]).
-include("pgq.hrl").

%%--------------------------------------------------------------------
%% @doc
%% @end
%%--------------------------------------------------------------------
-spec to_record(Event) -> Return when
      Event :: tuple() | [tuple()],
      Return :: #pgq_event{} | [#pgq_event{}].

to_record(List) 
  when is_list(List) ->
    Fun = fun(Event) -> to_record(Event) end,
    lists:map(Fun, List);
to_record({Event}) ->
    to_record(Event);
to_record({ Id, Time, TxId, Retry, Type, Data, Extra1, Extra2, Extra3, Extra4 }) ->
    #pgq_event{ id = Id
              , time = Time
              , txid = TxId
              , retry = Retry
              , type = Type
              , data = Data
              , extra1 = Extra1
              , extra2 = Extra2
              , extra3 = Extra3
              , extra4 = Extra4
              }.

