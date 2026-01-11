-module(sarc_upload_handler).

%% Cowboy REST callbacks
-export([init/2, allowed_methods/2, content_types_accepted/2, upload_from_binary/2]).

-define(STREAM_CHUNK_BYTES, 262144). % 256 KiB

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

    io:format("DEBUG: upload_from_binary zone=~p~n", [ZoneBin]),

    case sarc_handler_utils:parse_zone(ZoneBin) of
        {ok, Zone} ->
            %% Start streaming upload
            case sarc_port:put_stream_start(Zone) of
                {ok, Handle} ->
                    io:format("DEBUG: put_stream_start ok handle=~p~n", [Handle]),
                    case stream_body(Req0, Handle) of
                        {ok, Req1} ->
                            %% Finish upload
                            io:format("DEBUG: calling put_stream_finish~n"),
                            Result = sarc_port:put_stream_finish(Handle, Zone, Filename, MimeType),
                            io:format("DEBUG: put_stream_finish result=~p~n", [Result]),
                            case Result of
                                {ok, Key, Meta, Deduplicated} ->
                                    %% Broadcast to subscribers
                                    pg:join(sarc_pubsub, Zone, self()), %% Join simply to ensure group exists? No need.
                                    %% Actually pg:get_members(Scope, Group) returns pids.
                                    %% We want to send to group.
                                    %% pg doesn't have a 'publish' function, we iterate.
                                    Pids = pg:get_members(sarc_pubsub, Zone),
                                    [Pid ! {new_object, Key, Meta} || Pid <- Pids],

                                    Response = #{
                                        key => sarc_handler_utils:format_key(Key),
                                        meta => sarc_handler_utils:format_meta(Meta),
                                        deduplicated => Deduplicated
                                    },
                                    Json = jsx:encode(Response),
                                    Req2 = cowboy_req:reply(200, #{
                                        <<"content-type">> => <<"application/json">>
                                    }, Json, Req1),
                                    {stop, Req2, State};
                                {error, Reason} ->
                                    io:format("DEBUG: put_stream_finish error=~p~n", [Reason]),
                                    {Code, Msg} = sarc_handler_utils:map_error(Reason),
                                    Req2 = sarc_handler_utils:error_response(Req1, Code, Msg),
                                    {stop, Req2, State}
                            end;
                        {error, Reason, Req1} ->
                            io:format("DEBUG: stream_body error=~p~n", [Reason]),
                            catch sarc_port:put_stream_abort(Handle),
                            {Code, Msg} = sarc_handler_utils:map_error(Reason),
                            Req2 = sarc_handler_utils:error_response(Req1, Code, Msg),
                            {stop, Req2, State};
                        {error, _Reason} ->
                            io:format("DEBUG: stream_body error (unknown)~n"),
                            catch sarc_port:put_stream_abort(Handle),
                            Req1 = sarc_handler_utils:error_response(Req0, 500, <<"Upload stream failed">>),
                            {stop, Req1, State}
                    end;
                {error, Reason} ->
                    io:format("DEBUG: put_stream_start error=~p~n", [Reason]),
                    {Code, Msg} = sarc_handler_utils:map_error(Reason),
                    Req1 = sarc_handler_utils:error_response(Req0, Code, Msg),
                    {stop, Req1, State}
            end;
        {error, _} ->
            Req1 = sarc_handler_utils:error_response(Req0, 400, <<"Invalid or missing zone parameter">>),
            {stop, Req1, State}
    end.

stream_body(Req0, Handle) ->
    Opts = #{length => ?STREAM_CHUNK_BYTES, period => 5000},
    case cowboy_req:read_body(Req0, Opts) of
        {ok, Data, Req} ->
            case catch sarc_port:put_stream_chunk(Handle, Data) of
                ok -> {ok, Req};
                {error, Reason} -> {error, Reason, Req};
                {'EXIT', Reason} -> {error, Reason, Req}
            end;
        {more, Data, Req} ->
            case catch sarc_port:put_stream_chunk(Handle, Data) of
                ok -> stream_body(Req, Handle);
                {error, Reason} -> {error, Reason, Req};
                {'EXIT', Reason} -> {error, Reason, Req}
            end
    end.
