%
-module(sarc_zone_share_handler).

%% Cowboy REST callbacks
-export([init/2, allowed_methods/2, content_types_accepted/2, share_from_json/2]).

init(Req0, State) ->
    ZoneBin = cowboy_req:binding(zone, Req0),
    case sarc_handler_utils:parse_zone(ZoneBin) of
        {ok, Zone} ->
            {cowboy_rest, Req0, State#{zone => Zone}};
        {error, _} ->
            Req1 = sarc_handler_utils:error_response(Req0, 400, <<"Invalid zone">>),
            {stop, Req1, State}
    end.

allowed_methods(Req, State) ->
    {[<<"POST">>], Req, State}.

content_types_accepted(Req, State) ->
    {[
        {{<<"application">>, <<"json">>, '*'}, share_from_json},
        {'*', share_from_json}
    ], Req, State}.

share_from_json(Req0, State = #{zone := Zone}) ->
    {ok, Body, Req1} = sarc_handler_utils:read_body(Req0),
    BodyHash = sarc_auth:sha256_hex(Body),
    Path = sarc_auth:canonical_path(Req1),
    case sarc_auth:require_auth(Req1, <<"POST">>, Path, BodyHash, Zone, true) of
        {ok, User} ->
            try
                Data = jsx:decode(Body, [return_maps]),
                Shared = maps:get(<<"shared">>, Data, undefined),
                case Shared of
                    true ->
                        ok = sarc_auth:set_zone_shared(Zone, true),
                        sarc_auth:log_action(User, Zone, <<"zone_share">>, Req1, ok, #{shared => true}),
                        Req2 = sarc_handler_utils:json_response(Req1, #{zone => Zone, shared => true}),
                        {stop, Req2, State};
                    false ->
                        ok = sarc_auth:set_zone_shared(Zone, false),
                        sarc_auth:log_action(User, Zone, <<"zone_share">>, Req1, ok, #{shared => false}),
                        Req2 = sarc_handler_utils:json_response(Req1, #{zone => Zone, shared => false}),
                        {stop, Req2, State};
                    _ ->
                        sarc_auth:log_action(User, Zone, <<"zone_share">>, Req1, error, #{shared => Shared}),
                        Req2 = sarc_handler_utils:error_response(Req1, 400, <<"Missing or invalid shared flag">>),
                        {stop, Req2, State}
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
