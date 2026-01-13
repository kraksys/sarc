%
-module(sarc_zone_members_handler).

%% Cowboy REST callbacks
-export([init/2, allowed_methods/2, content_types_provided/2, members_to_json/2]).

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
    {[<<"GET">>], Req, State}.

content_types_provided(Req, State) ->
    {[
        {{<<"application">>, <<"json">>, '*'}, members_to_json},
        {{<<"text">>, <<"plain">>, '*'}, members_to_json}
    ], Req, State}.

members_to_json(Req0, State = #{zone := Zone}) ->
    Path = sarc_auth:canonical_path(Req0),
    BodyHash = sarc_auth:sha256_hex(<<>>),
    case sarc_auth:require_auth(Req0, <<"GET">>, Path, BodyHash, Zone, true) of
        {ok, User} ->
            Members = sarc_auth:list_zone_members(Zone),
            Format = proplists:get_value(<<"format">>, cowboy_req:parse_qs(Req0), <<"json">>),
            sarc_auth:log_action(User, Zone, <<"zone_members">>, Req0, ok, #{}),
            case Format of
                <<"plain">> ->
                    Lines = [format_member_line(M) || M <- Members],
                    Body = iolist_to_binary(Lines),
                    Req1 = cowboy_req:reply(200, #{<<"content-type">> => <<"text/plain">>}, Body, Req0),
                    {stop, Req1, State};
                _ ->
                    Req1 = sarc_handler_utils:json_response(Req0, #{zone => Zone, members => Members}),
                    {stop, Req1, State}
            end;
        {error, {Code, Msg}} ->
            Req1 = sarc_handler_utils:error_response(Req0, Code, Msg),
            {stop, Req1, State}
    end.

format_member_line(M) ->
    UserId = maps:get(user_id, M, 0),
    Label = sanitize_text(maps:get(label, M, <<"">>)),
    Finger = maps:get(fingerprint, M, <<"">>),
    io_lib:format("~B\t~s\t~s\n", [UserId, Label, Finger]).

sanitize_text(Bin) when is_binary(Bin) ->
    lists:map(fun
        ($\t) -> $ ;
        ($\n) -> $ ;
        ($\r) -> $ ;
        (C) -> C
    end, binary_to_list(Bin)).
