%
-module(sarc_auth_pair_approve_handler).

%% Cowboy REST callbacks
-export([init/2, allowed_methods/2, content_types_accepted/2, approve_from_json/2]).

init(Req0, State) ->
    {cowboy_rest, Req0, State}.

allowed_methods(Req, State) ->
    {[<<"POST">>], Req, State}.

content_types_accepted(Req, State) ->
    {[
        {{<<"application">>, <<"json">>, '*'}, approve_from_json},
        {'*', approve_from_json}
    ], Req, State}.

approve_from_json(Req0, State) ->
    {ok, Body, Req1} = sarc_handler_utils:read_body(Req0),
    BodyHash = sarc_auth:sha256_hex(Body),
    Path = sarc_auth:canonical_path(Req1),
    case sarc_auth:require_auth(Req1, <<"POST">>, Path, BodyHash, undefined, true) of
        {ok, User} ->
            try
                Data = jsx:decode(Body, [return_maps]),
                Code = maps:get(<<"code">>, Data, undefined),
                case Code of
                    undefined ->
                        Req2 = sarc_handler_utils:error_response(Req1, 400, <<"Missing code">>),
                        {stop, Req2, State};
                    _ ->
                        case sarc_auth:pair_approve(Code, User) of
                            {ok, Info} ->
                                sarc_auth:log_action(User, 0, <<"pair_approve">>, Req1, ok, #{code => Code}),
                                Req2 = sarc_handler_utils:json_response(Req1, Info),
                                {stop, Req2, State};
                            {error, expired} ->
                                sarc_auth:log_action(User, 0, <<"pair_approve">>, Req1, error, #{code => Code}),
                                Req2 = sarc_handler_utils:error_response(Req1, 410, <<"Pairing code expired">>),
                                {stop, Req2, State};
                            {error, invalid_code} ->
                                sarc_auth:log_action(User, 0, <<"pair_approve">>, Req1, error, #{code => Code}),
                                Req2 = sarc_handler_utils:error_response(Req1, 404, <<"Invalid code">>),
                                {stop, Req2, State};
                            {error, _} ->
                                sarc_auth:log_action(User, 0, <<"pair_approve">>, Req1, error, #{code => Code}),
                                Req2 = sarc_handler_utils:error_response(Req1, 500, <<"Pair approve failed">>),
                                {stop, Req2, State}
                        end
                end
            catch
                _:_ ->
                    ReqErr = sarc_handler_utils:error_response(Req1, 400, <<"Invalid JSON">>),
                    {stop, ReqErr, State}
            end;
        {error, {Code, Msg}} ->
            Req2 = sarc_handler_utils:error_response(Req1, Code, Msg),
            {stop, Req2, State}
    end.
