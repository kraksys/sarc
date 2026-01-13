%
-module(sarc_auth_init_handler).

%% Cowboy REST callbacks
-export([init/2, allowed_methods/2, content_types_accepted/2, init_from_json/2]).

init(Req, State) ->
    {cowboy_rest, Req, State}.

allowed_methods(Req, State) ->
    {[<<"POST">>], Req, State}.

content_types_accepted(Req, State) ->
    {[
        {{<<"application">>, <<"json">>, '*'}, init_from_json},
        {'*', init_from_json}
    ], Req, State}.

init_from_json(Req0, State) ->
    {ok, Body, Req} = sarc_handler_utils:read_body(Req0),
    try
        Data = jsx:decode(Body, [return_maps]),
        PubB64 = maps:get(<<"pubkey">>, Data, undefined),
        Label = maps:get(<<"label">>, Data, <<"">>),
        case PubB64 of
            undefined ->
                Req1 = sarc_handler_utils:error_response(Req, 400, <<"Missing pubkey">>),
                {stop, Req1, State};
            _ ->
                case base64:decode(PubB64) of
                    PubKey when is_binary(PubKey) ->
                        case sarc_auth:init_admin(PubKey, Label) of
                            {ok, Info} ->
                                Req1 = sarc_handler_utils:json_response(Req, Info),
                                {stop, Req1, State};
                            {error, already_initialized} ->
                                Req1 = sarc_handler_utils:error_response(Req, 409, <<"Admin already initialized">>),
                                {stop, Req1, State};
                            {error, _} ->
                                Req1 = sarc_handler_utils:error_response(Req, 500, <<"Init failed">>),
                                {stop, Req1, State}
                        end;
                    _ ->
                        Req1 = sarc_handler_utils:error_response(Req, 400, <<"Invalid pubkey">>),
                        {stop, Req1, State}
                end
        end
    catch
        _:_ ->
            ReqErr = sarc_handler_utils:error_response(Req, 400, <<"Invalid JSON">>),
            {stop, ReqErr, State}
    end.
