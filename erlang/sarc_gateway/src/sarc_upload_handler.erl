-module(sarc_upload_handler).

%% Cowboy REST callbacks
-export([init/2, allowed_methods/2, content_types_accepted/2, upload_from_binary/2]).

init(Req, State) ->
    {cowboy_rest, Req, State}.

allowed_methods(Req, State) ->
    {[<<"PUT">>], Req, State}.

content_types_accepted(Req, State) ->
    {[
        {{<<"application">>, <<"octet-stream">>, '*'}, upload_from_binary},
        {{<<"text">>, <<"plain">>, '*'}, upload_from_binary},
        {'*', upload_from_binary}
    ], Req, State}.

upload_from_binary(Req0, State) ->
    %% Parse query parameters
    QsVals = cowboy_req:parse_qs(Req0),
    ZoneBin = proplists:get_value(<<"zone">>, QsVals),
    Filename = proplists:get_value(<<"filename">>, QsVals, <<>>),
    MimeType = proplists:get_value(<<"mime_type">>, QsVals, <<"application/octet-stream">>),

    case sarc_handler_utils:parse_zone(ZoneBin) of
        {ok, Zone} ->
            %% Read request body
            {ok, Body, Req1} = sarc_handler_utils:read_body(Req0),

            %% Call port to store object
            case sarc_port:object_put(Zone, Body, Filename, MimeType) of
                {ok, Key, Meta, Deduplicated} ->
                    Response = #{
                        key => sarc_handler_utils:format_key(Key),
                        meta => sarc_handler_utils:format_meta(Meta),
                        deduplicated => Deduplicated
                    },
                    Json = jsx:encode(Response),
                    Req2 = cowboy_req:set_resp_body(Json, Req1),
                    Req3 = cowboy_req:set_resp_header(<<"content-type">>,
                                                       <<"application/json">>, Req2),
                    {true, Req3, State};
                {error, Reason} ->
                    {Code, Msg} = sarc_handler_utils:map_error(Reason),
                    Req2 = sarc_handler_utils:error_response(Req1, Code, Msg),
                    {false, Req2, State}
            end;
        {error, _} ->
            Req1 = sarc_handler_utils:error_response(Req0, 400, <<"Invalid or missing zone parameter">>),
            {false, Req1, State}
    end.
