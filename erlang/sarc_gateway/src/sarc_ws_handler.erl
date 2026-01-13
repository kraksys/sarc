-module(sarc_ws_handler).

%% Cowboy WebSocket callbacks
-export([init/2, websocket_init/1, websocket_handle/2, websocket_info/2, terminate/3]).

-define(HEARTBEAT_INTERVAL, 30000).  % 30 seconds

%%====================================================================
%% Cowboy WebSocket callbacks
%%====================================================================

init(Req, State) ->
    %% Upgrade to WebSocket
    io:format("DEBUG: WS init~n"),
    {cowboy_websocket, Req, State}.

websocket_init(State) ->
    %% Start heartbeat timer
    io:format("DEBUG: WS websocket_init~n"),
    erlang:send_after(?HEARTBEAT_INTERVAL, self(), heartbeat),
    {ok, State}.

websocket_handle({text, Msg}, State) ->
    %% Parse JSON message
    io:format("DEBUG: WS received: ~p~n", [Msg]),
    try
        Request = jsx:decode(Msg, [return_maps]),
        handle_ws_request(Request, State)
    catch
        _:_ ->
            ErrorMsg = jsx:encode(#{type => error, reason => <<"Invalid JSON">>}),
            {reply, {text, ErrorMsg}, State}
    end;

websocket_handle({binary, _Data}, State) ->
    %% Binary frames not supported in this simple implementation
    ErrorMsg = jsx:encode(#{type => error, reason => <<"Binary frames not supported">>}),
    {reply, {text, ErrorMsg}, State};

websocket_handle(_Frame, State) ->
    {ok, State}.

websocket_info({new_object, Key, Meta}, State) ->
    Response = #{
        type => new_object,
        key => sarc_handler_utils:format_key(Key),
        meta => sarc_handler_utils:format_meta(Meta)
    },
    {reply, {text, jsx:encode(Response)}, State};

websocket_info(heartbeat, State) ->
    %% Send ping and schedule next heartbeat
    erlang:send_after(?HEARTBEAT_INTERVAL, self(), heartbeat),
    {reply, ping, State};

websocket_info(_Info, State) ->
    {ok, State}.

terminate(_Reason, _Req, _State) ->
    ok.

%%====================================================================
%% Request handling
%%====================================================================

handle_ws_request(#{<<"auth">> := AuthMap, <<"payload_b64">> := PayloadB64}, State) ->
    case parse_ws_auth(AuthMap) of
        {ok, Auth} ->
            case base64:decode(PayloadB64) of
                PayloadBin when is_binary(PayloadBin) ->
                    BodyHash = sarc_auth:sha256_hex(PayloadBin),
                    Path = <<"/ws">>,
                    case jsx:decode(PayloadBin, [return_maps]) of
                        Payload when is_map(Payload) ->
                            Zone = maps:get(<<"zone">>, Payload, undefined),
                            case sarc_auth:precheck(Auth, Zone, false) of
                                {ok, User} ->
                                    case sarc_auth:verify_signature(Auth, <<"WS">>, Path, BodyHash) of
                                        ok ->
                                            handle_request(Payload, User, State);
                                        {error, {_Code, Msg}} ->
                                            ErrorMsg = jsx:encode(#{type => error, reason => Msg}),
                                            {reply, {text, ErrorMsg}, State}
                                    end;
                                {error, {_Code, Msg}} ->
                                    ErrorMsg = jsx:encode(#{type => error, reason => Msg}),
                                    {reply, {text, ErrorMsg}, State}
                            end;
                        _ ->
                            ErrorMsg = jsx:encode(#{type => error, reason => <<"Invalid payload">>}),
                            {reply, {text, ErrorMsg}, State}
                    end;
                _ ->
                    ErrorMsg = jsx:encode(#{type => error, reason => <<"Invalid payload encoding">>}),
                    {reply, {text, ErrorMsg}, State}
            end;
        {error, Msg} ->
            ErrorMsg = jsx:encode(#{type => error, reason => Msg}),
            {reply, {text, ErrorMsg}, State}
    end;

handle_ws_request(_Request, State) ->
    ErrorMsg = jsx:encode(#{type => error, reason => <<"Missing auth or payload">>}),
    {reply, {text, ErrorMsg}, State}.

handle_request(#{<<"action">> := <<"subscribe">>, <<"zone">> := Zone}, User, State) ->
    io:format("DEBUG: WS subscribing to zone ~p~n", [Zone]),
    pg:join(sarc_pubsub, Zone, self()),
    Response = #{type => result, status => subscribed, zone => Zone},
    sarc_auth:log_action(User, Zone, <<"ws_subscribe">>, undefined, ok, #{}),
    {reply, {text, jsx:encode(Response)}, State};

handle_request(#{<<"action">> := <<"get">>, <<"zone">> := Zone, <<"hash">> := HashHex}, User, State) ->
    case sarc_handler_utils:parse_zone_and_hash(
           integer_to_binary(Zone), HashHex) of
        {ok, ZoneId, Hash} ->
            case sarc_nif:object_get_nif(ZoneId, Hash) of
                {ok, Data, Meta} ->
                    %% Encode data as base64
                    DataB64 = base64:encode(Data),
                    Response = #{
                        type => data,
                        data => DataB64,
                        meta => sarc_handler_utils:format_meta(Meta)
                    },
                    sarc_auth:log_action(User, ZoneId, <<"ws_get">>, undefined, ok, #{hash => HashHex}),
                    {reply, {text, jsx:encode(Response)}, State};
                {error, Reason} ->
                    ErrorMsg = jsx:encode(#{
                        type => error,
                        reason => format_error(Reason)
                    }),
                    sarc_auth:log_action(User, ZoneId, <<"ws_get">>, undefined, error, #{reason => Reason}),
                    {reply, {text, ErrorMsg}, State}
            end;
        {error, _} ->
            ErrorMsg = jsx:encode(#{type => error, reason => <<"Invalid zone or hash">>}),
            {reply, {text, ErrorMsg}, State}
    end;

handle_request(#{<<"action">> := <<"put">>,
                 <<"zone">> := Zone,
                 <<"data">> := DataB64} = Req, User, State) ->
    try
        %% Decode base64 data
        Data = base64:decode(DataB64),
        Filename = maps:get(<<"filename">>, Req, <<>>),
        MimeType = maps:get(<<"mime_type">>, Req, <<"application/octet-stream">>),

        case sarc_port:object_put(Zone, Data, Filename, MimeType) of
            {ok, Key, Meta, Deduplicated} ->
                Response = #{
                    type => result,
                    key => sarc_handler_utils:format_key(Key),
                    meta => sarc_handler_utils:format_meta(Meta),
                    deduplicated => Deduplicated
                },
                sarc_auth:log_action(User, Zone, <<"ws_put">>, undefined, ok, #{filename => Filename, mime_type => MimeType}),
                {reply, {text, jsx:encode(Response)}, State};
            {error, Reason} ->
                ErrorMsg = jsx:encode(#{
                    type => error,
                    reason => format_error(Reason)
                }),
                sarc_auth:log_action(User, Zone, <<"ws_put">>, undefined, error, #{reason => Reason}),
                {reply, {text, ErrorMsg}, State}
        end
    catch
        _:_ ->
            CatchError = jsx:encode(#{type => error, reason => <<"Invalid base64 data">>}),
            {reply, {text, CatchError}, State}
    end;

handle_request(#{<<"action">> := <<"delete">>, <<"zone">> := Zone, <<"hash">> := HashHex}, User, State) ->
    case sarc_handler_utils:parse_zone_and_hash(
           integer_to_binary(Zone), HashHex) of
        {ok, ZoneId, Hash} ->
            case sarc_port:object_delete(ZoneId, Hash) of
                ok ->
                    Response = #{type => result, status => ok},
                    sarc_auth:log_action(User, ZoneId, <<"ws_delete">>, undefined, ok, #{hash => HashHex}),
                    {reply, {text, jsx:encode(Response)}, State};
                {error, Reason} ->
                    ErrorMsg = jsx:encode(#{
                        type => error,
                        reason => format_error(Reason)
                    }),
                    sarc_auth:log_action(User, ZoneId, <<"ws_delete">>, undefined, error, #{reason => Reason}),
                    {reply, {text, ErrorMsg}, State}
            end;
        {error, _} ->
            ErrorMsg = jsx:encode(#{type => error, reason => <<"Invalid zone or hash">>}),
            {reply, {text, ErrorMsg}, State}
    end;

handle_request(#{<<"action">> := <<"query">>, <<"zone">> := Zone} = Req, User, State) ->
    Filter = maps:get(<<"filter">>, Req, #{}),
    Limit = maps:get(<<"limit">>, Req, 100),

    case sarc_nif:object_query_nif(Zone, Filter, Limit) of
        {ok, Results} ->
            Response = #{
                type => result,
                results => [sarc_handler_utils:format_key({Z, H}) || {Z, H} <- Results],
                count => length(Results)
            },
            sarc_auth:log_action(User, Zone, <<"ws_query">>, undefined, ok, #{count => length(Results)}),
            {reply, {text, jsx:encode(Response)}, State};
        {error, Reason} ->
            ErrorMsg = jsx:encode(#{
                type => error,
                reason => format_error(Reason)
            }),
            sarc_auth:log_action(User, Zone, <<"ws_query">>, undefined, error, #{reason => Reason}),
            {reply, {text, ErrorMsg}, State}
    end;

handle_request(_Request, _User, State) ->
    ErrorMsg = jsx:encode(#{type => error, reason => <<"Unknown action">>}),
    {reply, {text, ErrorMsg}, State}.

parse_ws_auth(#{<<"pub">> := PubB64, <<"sig">> := SigB64, <<"ts">> := TsBin, <<"nonce">> := NonceBin}) ->
    case {b64_decode(PubB64), b64_decode(SigB64), parse_ts(TsBin), validate_nonce(NonceBin)} of
        {{ok, PubKey}, {ok, Sig}, {ok, Ts}, ok} ->
            {ok, #{
                pubkey => PubKey,
                pubhash => crypto:hash(sha256, PubKey),
                sig => Sig,
                ts => Ts,
                ts_bin => TsBin,
                nonce => NonceBin
            }};
        _ ->
            {error, <<"Invalid auth">>}
    end;
parse_ws_auth(_) ->
    {error, <<"Invalid auth">>}.

b64_decode(Bin) when is_binary(Bin) ->
    try {ok, base64:decode(Bin)} catch _:_ -> {error, invalid_b64} end.

parse_ts(TsBin) ->
    try {ok, binary_to_integer(TsBin)} catch _:_ -> {error, invalid_ts} end.

validate_nonce(NonceBin) when is_binary(NonceBin) ->
    case byte_size(NonceBin) of
        32 ->
            case is_hex_binary(NonceBin) of
                true -> ok;
                false -> {error, invalid_nonce}
            end;
        _ -> {error, invalid_nonce}
    end.

is_hex_binary(Bin) ->
    lists:all(fun(C) ->
        (C >= $0 andalso C =< $9) orelse
        (C >= $a andalso C =< $f) orelse
        (C >= $A andalso C =< $F)
    end, binary_to_list(Bin)).

%%====================================================================
%% Helper functions
%%====================================================================

format_error(not_found) -> <<"not_found">>;
format_error(invalid) -> <<"invalid">>;
format_error(io) -> <<"io_error">>;
format_error(corrupt) -> <<"corrupt">>;
format_error(permission_denied) -> <<"permission_denied">>;
format_error(unavailable) -> <<"unavailable">>;
format_error(_) -> <<"unknown_error">>.
