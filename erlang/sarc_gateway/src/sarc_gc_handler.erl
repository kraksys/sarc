-module(sarc_gc_handler).

%% Cowboy REST callbacks
-export([init/2, allowed_methods/2, content_types_accepted/2, gc_from_json/2]).

init(Req, State) ->
    Zone = cowboy_req:binding(zone, Req),
    {cowboy_rest, Req, State#{zone => Zone}}.

allowed_methods(Req, State) ->
    {[<<"POST">>], Req, State}.

content_types_accepted(Req, State) ->
    {[
        {{<<"application">>, <<"json">>, '*'}, gc_from_json},
        {'*', gc_from_json}  % Allow POST without content-type
    ], Req, State}.

gc_from_json(Req, State = #{zone := ZoneBin}) ->
    case sarc_handler_utils:parse_zone(ZoneBin) of
        {ok, Zone} ->
            case sarc_port:object_gc(Zone) of
                {ok, DeletedCount} ->
                    Response = #{deleted_count => DeletedCount},
                    Json = jsx:encode(Response),
                    Req1 = cowboy_req:set_resp_body(Json, Req),
                    Req2 = cowboy_req:set_resp_header(<<"content-type">>,
                                                       <<"application/json">>, Req1),
                    {true, Req2, State};
                {error, Reason} ->
                    {Code, Msg} = sarc_handler_utils:map_error(Reason),
                    {false, sarc_handler_utils:error_response(Req, Code, Msg), State}
            end;
        {error, _} ->
            {false, sarc_handler_utils:error_response(Req, 400, <<"Invalid zone">>), State}
    end.
