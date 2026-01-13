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
            Path = sarc_auth:canonical_path(Req0),
            case sarc_auth:parse_auth_headers(Req0) of
                {ok, Auth} ->
                    case sarc_auth:precheck(Auth, Zone, false) of
                        {ok, User} ->
                            %% Start streaming upload
                            case sarc_port:put_stream_start(Zone) of
                                {ok, Handle} ->
                                    io:format("DEBUG: put_stream_start ok handle=~p~n", [Handle]),
                                    HashState = crypto:hash_init(sha256),
                                    case stream_body(Req0, Handle, HashState) of
                                        {ok, Req1, BodyHash} ->
                                            case sarc_auth:verify_signature(Auth, <<"PUT">>, Path, BodyHash) of
                                                ok ->
                                                    %% Finish upload
                                                    io:format("DEBUG: calling put_stream_finish~n"),
                                                    Result = sarc_port:put_stream_finish(Handle, Zone, Filename, MimeType),
                                                    io:format("DEBUG: put_stream_finish result=~p~n", [Result]),
                                                    case Result of
                                                        {ok, Key, Meta, Deduplicated} ->
                                                            %% Broadcast to subscribers
                                                            Pids = pg:get_members(sarc_pubsub, Zone),
                                                            [Pid ! {new_object, Key, Meta} || Pid <- Pids],

                                                            KeyMap = sarc_handler_utils:format_key(Key),
                                                            sarc_auth:log_action(User, Zone, <<"put">>, Req1, ok, #{
                                                                hash => maps:get(hash, KeyMap, <<"">>),
                                                                filename => Filename,
                                                                mime_type => MimeType,
                                                                size => maps:get(size, Meta, 0)
                                                            }),

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
                                                            sarc_auth:log_action(User, Zone, <<"put">>, Req1, error, #{reason => Reason}),
                                                            {Code, Msg} = sarc_handler_utils:map_error(Reason),
                                                            Req2 = sarc_handler_utils:error_response(Req1, Code, Msg),
                                                            {stop, Req2, State}
                                                    end;
                                                {error, {Code, Msg}} ->
                                                    catch sarc_port:put_stream_abort(Handle),
                                                    sarc_auth:log_action(User, Zone, <<"put">>, Req1, error, #{reason => Msg}),
                                                    Req2 = sarc_handler_utils:error_response(Req1, Code, Msg),
                                                    {stop, Req2, State}
                                            end;
                                        {error, Reason, Req1} ->
                                            io:format("DEBUG: stream_body error=~p~n", [Reason]),
                                            catch sarc_port:put_stream_abort(Handle),
                                            sarc_auth:log_action(User, Zone, <<"put">>, Req1, error, #{reason => Reason}),
                                            {Code, Msg} = sarc_handler_utils:map_error(Reason),
                                            Req2 = sarc_handler_utils:error_response(Req1, Code, Msg),
                                            {stop, Req2, State};
                                        {error, _Reason} ->
                                            io:format("DEBUG: stream_body error (unknown)~n"),
                                            catch sarc_port:put_stream_abort(Handle),
                                            sarc_auth:log_action(User, Zone, <<"put">>, Req0, error, #{reason => <<"stream_failed">>}),
                                            Req1 = sarc_handler_utils:error_response(Req0, 500, <<"Upload stream failed">>),
                                            {stop, Req1, State}
                                    end;
                                {error, Reason} ->
                                    io:format("DEBUG: put_stream_start error=~p~n", [Reason]),
                                    sarc_auth:log_action(User, Zone, <<"put">>, Req0, error, #{reason => Reason}),
                                    {Code, Msg} = sarc_handler_utils:map_error(Reason),
                                    Req1 = sarc_handler_utils:error_response(Req0, Code, Msg),
                                    {stop, Req1, State}
                            end;
                        {error, {Code, Msg}} ->
                            Req1 = sarc_handler_utils:error_response(Req0, Code, Msg),
                            {stop, Req1, State}
                    end;
                {error, {Code, Msg}} ->
                    Req1 = sarc_handler_utils:error_response(Req0, Code, Msg),
                    {stop, Req1, State}
            end;
        {error, _} ->
            Req1 = sarc_handler_utils:error_response(Req0, 400, <<"Invalid or missing zone parameter">>),
            {stop, Req1, State}
    end.

stream_body(Req0, Handle, HashState0) ->
    Opts = #{length => ?STREAM_CHUNK_BYTES, period => 5000},
    case cowboy_req:read_body(Req0, Opts) of
        {ok, Data, Req} ->
            case catch sarc_port:put_stream_chunk(Handle, Data) of
                ok ->
                    HashState = crypto:hash_update(HashState0, Data),
                    BodyHash = list_to_binary(sarc_codec:binary_to_hex(crypto:hash_final(HashState))),
                    {ok, Req, BodyHash};
                {error, Reason} -> {error, Reason, Req};
                {'EXIT', Reason} -> {error, Reason, Req}
            end;
        {more, Data, Req} ->
            case catch sarc_port:put_stream_chunk(Handle, Data) of
                ok ->
                    HashState = crypto:hash_update(HashState0, Data),
                    stream_body(Req, Handle, HashState);
                {error, Reason} -> {error, Reason, Req};
                {'EXIT', Reason} -> {error, Reason, Req}
            end
    end.
