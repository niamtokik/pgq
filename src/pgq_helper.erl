-module(pgq_helper).
-export([format/2]).

format(Pattern, Data) ->
    io:format("~p", [Data]).
