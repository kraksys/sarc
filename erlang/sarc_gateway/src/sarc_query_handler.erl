-module(sarc_query_handler).

%% Cowboy REST callbacks
-export([init/2, allowed_methods/2, content_types_provided/2, query_to_json/2]).

init(Req, State) ->
    Zone = cowboy_req:binding(zone, Req),
    {cowboy_rest, Req, State#{zone => Zone}}.

allowed_methods(Req, State) ->
    {[<<"GET">>], Req, State}.

content_types_provided(Req, State) ->
    {[
        {{<<"application">>, <<"json">>, '*'}, query_to_json}
    ], Req, State}.

query_to_json(Req, State = #{zone := ZoneBin}) ->
    io:format("DEBUG: query_to_json called for zone ~p~n", [ZoneBin]),
    case sarc_handler_utils:parse_zone(ZoneBin) of
        {ok, Zone} ->
            %% Parse query parameters
            QsVals = cowboy_req:parse_qs(Req),
            Filter = sarc_handler_utils:build_filter(QsVals),
            Limit = sarc_handler_utils:parse_limit(
                proplists:get_value(<<"limit">>, QsVals, <<"100">>)),
            Format = proplists:get_value(<<"format">>, QsVals, <<"json">>),
            BodyHash = sarc_auth:sha256_hex(<<>>),
            Path = sarc_auth:canonical_path(Req),

            io:format("DEBUG: Calling sarc_nif:object_query_nif(~p, ~p, ~p)~n", [Zone, Filter, Limit]),
            case sarc_auth:require_auth(Req, <<"GET">>, Path, BodyHash, Zone, false) of
                {ok, User} ->
                    Result = sarc_nif:object_query_nif(Zone, Filter, Limit),
                    io:format("DEBUG: sarc_nif result: ~p~n", [Result]),
                    case Result of
                        {ok, Results} ->
                            io:format("DEBUG: Formatting results...~n"),
                            Formatted = [sarc_handler_utils:format_key(Item) || Item <- Results],
                            sarc_auth:log_action(User, Zone, <<"query">>, Req, ok, #{count => length(Results)}),
                            case Format of
                                <<"plain">> ->
                                    Body = format_plain_results(Formatted),
                                    Req1 = cowboy_req:reply(200, #{<<"content-type">> => <<"text/plain">>}, Body, Req),
                                    {halt, Req1, State};
                                _ ->
                                    Response = #{
                                        results => Formatted,
                                        count => length(Results)
                                    },
                                    Json = jsx:encode(Response),
                                    {Json, Req, State}
                            end;
                        {error, Reason} ->
                            io:format("DEBUG: Error reason: ~p~n", [Reason]),
                            sarc_auth:log_action(User, Zone, <<"query">>, Req, error, #{reason => Reason}),
                            {Code, Msg} = sarc_handler_utils:map_error(Reason),
                            {halt, cowboy_req:reply(Code, #{}, Msg, Req), State}
                    end;
                {error, {Code, Msg}} ->
                    Req1 = sarc_handler_utils:error_response(Req, Code, Msg),
                    {halt, Req1, State}
            end;
        {error, _} ->
            {halt, cowboy_req:reply(400, #{}, <<"Invalid zone">>, Req), State}
    end.

format_plain_results(Formatted) ->
    Lines = format_plain_lines(Formatted, 1, []),
    iolist_to_binary(lists:reverse(Lines)).

format_plain_lines([], _Rid, Acc) ->
    Acc;
format_plain_lines([Item | Rest], Rid, Acc) ->
    Zone = maps:get(zone, Item, 0),
    Hash = maps:get(hash, Item, <<"">>),
    Size = maps:get(size, Item, 0),
    Filename = sanitize_text(maps:get(filename, Item, <<"">>)),
    Mime = sanitize_text(maps:get(mime_type, Item, <<"">>)),
    Line = io_lib:format("~B\t~B\t~s\t~B\t~s\t~s\n", [Rid, Zone, Hash, Size, Filename, Mime]),
    format_plain_lines(Rest, Rid + 1, [Line | Acc]).

sanitize_text(Bin) when is_binary(Bin) ->
    lists:map(fun
        ($\t) -> $ ;
        ($\n) -> $ ;
        ($\r) -> $ ;
        (C) -> C
    end, binary_to_list(Bin)).
