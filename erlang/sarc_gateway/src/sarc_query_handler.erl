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
    case sarc_handler_utils:parse_zone(ZoneBin) of
        {ok, Zone} ->
            %% Parse query parameters
            QsVals = cowboy_req:parse_qs(Req),
            Filter = sarc_handler_utils:build_filter(QsVals),
            Limit = sarc_handler_utils:parse_limit(
                proplists:get_value(<<"limit">>, QsVals, <<"100">>)),

            case sarc_nif:object_query_nif(Zone, Filter, Limit) of
                {ok, Results} ->
                    Response = #{
                        results => [sarc_handler_utils:format_key({Z, H}) || {Z, H} <- Results],
                        count => length(Results)
                    },
                    Json = jsx:encode(Response),
                    {Json, Req, State};
                {error, Reason} ->
                    {Code, Msg} = sarc_handler_utils:map_error(Reason),
                    {halt, cowboy_req:reply(Code, #{}, Msg, Req), State}
            end;
        {error, _} ->
            {halt, cowboy_req:reply(400, #{}, <<"Invalid zone">>, Req), State}
    end.
