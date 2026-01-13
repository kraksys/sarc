%
-module(sarc_zone_audit_handler).

%% Cowboy REST callbacks
-export([init/2, allowed_methods/2, content_types_provided/2, audit_to_json/2]).

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
        {{<<"application">>, <<"json">>, '*'}, audit_to_json},
        {{<<"text">>, <<"plain">>, '*'}, audit_to_json}
    ], Req, State}.

audit_to_json(Req0, State = #{zone := Zone}) ->
    Path = sarc_auth:canonical_path(Req0),
    BodyHash = sarc_auth:sha256_hex(<<>>),
    case sarc_auth:require_auth(Req0, <<"GET">>, Path, BodyHash, Zone, true) of
        {ok, User} ->
            Qs = cowboy_req:parse_qs(Req0),
            Limit = sarc_handler_utils:parse_limit(proplists:get_value(<<"limit">>, Qs, <<"100">>)),
            Entries = sarc_auth:list_zone_audit(Zone, Limit),
            Format = proplists:get_value(<<"format">>, Qs, <<"json">>),
            sarc_auth:log_action(User, Zone, <<"zone_audit">>, Req0, ok, #{limit => Limit}),
            case Format of
                <<"plain">> ->
                    Lines = [format_audit_line(E) || E <- Entries],
                    Body = iolist_to_binary(Lines),
                    Req1 = cowboy_req:reply(200, #{<<"content-type">> => <<"text/plain">>}, Body, Req0),
                    {stop, Req1, State};
                _ ->
                    Req1 = sarc_handler_utils:json_response(Req0, #{zone => Zone, entries => Entries}),
                    {stop, Req1, State}
            end;
        {error, {Code, Msg}} ->
            Req1 = sarc_handler_utils:error_response(Req0, Code, Msg),
            {stop, Req1, State}
    end.

format_audit_line(E) ->
    Ts = maps:get(ts, E, 0),
    UserId = maps:get(user_id, E, 0),
    Action = maps:get(action, E, <<"">>),
    Status = maps:get(status, E, <<"">>),
    Meta = maps:get(meta, E, #{}),
    Hash = maps:get(hash, Meta, <<"">>),
    Filename = sanitize_text(maps:get(filename, Meta, <<"">>)),
    io_lib:format("~B\t~B\t~s\t~s\t~s\t~s\n", [Ts, UserId, Action, Status, Hash, Filename]).

sanitize_text(Bin) when is_binary(Bin) ->
    lists:map(fun
        ($\t) -> $ ;
        ($\n) -> $ ;
        ($\r) -> $ ;
        (C) -> C
    end, binary_to_list(Bin)).
