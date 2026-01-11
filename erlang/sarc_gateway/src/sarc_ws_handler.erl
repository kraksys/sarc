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
        handle_request(Request, State)
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

handle_request(#{<<"action">> := <<"subscribe">>, <<"zone">> := Zone}, State) ->
    io:format("DEBUG: WS subscribing to zone ~p~n", [Zone]),
    pg:join(sarc_pubsub, Zone, self()),
    Response = #{type => result, status => subscribed, zone => Zone},
    {reply, {text, jsx:encode(Response)}, State};

handle_request(#{<<"action">> := <<"get">>, <<"zone">> := Zone, <<"hash">> := HashHex}, State) ->
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
                    {reply, {text, jsx:encode(Response)}, State};
                {error, Reason} ->
                    ErrorMsg = jsx:encode(#{
                        type => error,
                        reason => format_error(Reason)
                    }),
                    {reply, {text, ErrorMsg}, State}
            end;
        {error, _} ->
            ErrorMsg = jsx:encode(#{type => error, reason => <<"Invalid zone or hash">>}),
            {reply, {text, ErrorMsg}, State}
    end;

handle_request(#{<<"action">> := <<"put">>,
                 <<"zone">> := Zone,
                 <<"data">> := DataB64} = Req, State) ->
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
                {reply, {text, jsx:encode(Response)}, State};
            {error, Reason} ->
                ErrorMsg = jsx:encode(#{
                    type => error,
                    reason => format_error(Reason)
                }),
                {reply, {text, ErrorMsg}, State}
        end
    catch
        _:_ ->
            CatchError = jsx:encode(#{type => error, reason => <<"Invalid base64 data">>}),
            {reply, {text, CatchError}, State}
    end;

handle_request(#{<<"action">> := <<"delete">>, <<"zone">> := Zone, <<"hash">> := HashHex}, State) ->
    case sarc_handler_utils:parse_zone_and_hash(
           integer_to_binary(Zone), HashHex) of
        {ok, ZoneId, Hash} ->
            case sarc_port:object_delete(ZoneId, Hash) of
                ok ->
                    Response = #{type => result, status => ok},
                    {reply, {text, jsx:encode(Response)}, State};
                {error, Reason} ->
                    ErrorMsg = jsx:encode(#{
                        type => error,
                        reason => format_error(Reason)
                    }),
                    {reply, {text, ErrorMsg}, State}
            end;
        {error, _} ->
            ErrorMsg = jsx:encode(#{type => error, reason => <<"Invalid zone or hash">>}),
            {reply, {text, ErrorMsg}, State}
    end;

handle_request(#{<<"action">> := <<"query">>, <<"zone">> := Zone} = Req, State) ->
    Filter = maps:get(<<"filter">>, Req, #{}),
    Limit = maps:get(<<"limit">>, Req, 100),

    case sarc_nif:object_query_nif(Zone, Filter, Limit) of
        {ok, Results} ->
            Response = #{
                type => result,
                results => [sarc_handler_utils:format_key({Z, H}) || {Z, H} <- Results],
                count => length(Results)
            },
            {reply, {text, jsx:encode(Response)}, State};
        {error, Reason} ->
            ErrorMsg = jsx:encode(#{
                type => error,
                reason => format_error(Reason)
            }),
            {reply, {text, ErrorMsg}, State}
    end;

handle_request(_Request, State) ->
    ErrorMsg = jsx:encode(#{type => error, reason => <<"Unknown action">>}),
    {reply, {text, ErrorMsg}, State}.

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
