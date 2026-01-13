-module(sarc_object_handler).

%% Cowboy REST callbacks
-export([init/2, allowed_methods/2, resource_exists/2, content_types_provided/2,
         delete_resource/2, provide_object/2]).

init(Req0, State) ->
    ZoneBin = cowboy_req:binding(zone, Req0),
    Hash = cowboy_req:binding(hash, Req0),
    case sarc_handler_utils:parse_zone(ZoneBin) of
        {ok, Zone} ->
            Method = cowboy_req:method(Req0),
            Path = sarc_auth:canonical_path(Req0),
            BodyHash = sarc_auth:sha256_hex(<<>>),
            case sarc_auth:require_auth(Req0, Method, Path, BodyHash, Zone, false) of
                {ok, User} ->
                    {cowboy_rest, Req0, State#{zone => ZoneBin, hash => Hash, auth_user => User, zone_id => Zone}};
                {error, {Code, Msg}} ->
                    Req1 = sarc_handler_utils:error_response(Req0, Code, Msg),
                    {stop, Req1, State}
            end;
        {error, _} ->
            Req1 = sarc_handler_utils:error_response(Req0, 400, <<"Invalid zone">>),
            {stop, Req1, State}
    end.

allowed_methods(Req, State) ->
    {[<<"GET">>, <<"DELETE">>], Req, State}.

resource_exists(Req, State = #{zone := ZoneBin, hash := HashBin}) ->
    case sarc_handler_utils:parse_zone_and_hash(ZoneBin, HashBin) of
        {ok, Zone, Hash} ->
            case sarc_nif:object_exists_nif(Zone, Hash) of
                {ok, true} ->
                    {true, Req, State#{zone_id => Zone, hash_bin => Hash}};
                {ok, false} ->
                    {false, Req, State};
                {error, _} ->
                    {false, Req, State}
            end;
        {error, _} ->
            {false, Req, State}
    end.

content_types_provided(Req, State) ->
    {[
        {{<<"application">>, <<"octet-stream">>, '*'}, provide_object}
    ], Req, State}.

provide_object(Req, State = #{zone_id := Zone, hash_bin := Hash}) ->
    case sarc_nif:object_get_metadata(Zone, Hash) of
        {ok, Meta = #{size := Size}} when Size > 1024 * 1024 ->
            %% Large file: use sendfile
            Path0 = maps:get(fs_path, Meta),
            Path = case Path0 of
                Bin when is_binary(Bin) -> binary_to_list(Bin);
                _ -> Path0
            end,
            Mime = maps:get(mime_type, Meta, <<"application/octet-stream">>),
            Headers = #{
                <<"content-type">> => Mime,
                <<"content-length">> => integer_to_binary(Size),
                <<"etag">> => sarc_handler_utils:hash_to_hex(Hash)
            },
            Req1 = cowboy_req:reply(200, Headers, {sendfile, 0, Size, Path}, Req),
            sarc_auth:log_action(maps:get(auth_user, State, #{}), Zone, <<"get">>, Req, ok, #{hash => sarc_handler_utils:hash_to_hex(Hash)}),
            {stop, Req1, State};

        {ok, _Meta} ->
            %% Small file: read into memory
            case sarc_nif:object_get_nif(Zone, Hash) of
                {ok, Data, _} ->
                    Req1 = cowboy_req:set_resp_header(<<"etag">>,
                                                       sarc_handler_utils:hash_to_hex(Hash), Req),
                    sarc_auth:log_action(maps:get(auth_user, State, #{}), Zone, <<"get">>, Req, ok, #{hash => sarc_handler_utils:hash_to_hex(Hash)}),
                    {Data, Req1, State};
                {error, Reason} ->
                    {Code, Msg} = sarc_handler_utils:map_error(Reason),
                    sarc_auth:log_action(maps:get(auth_user, State, #{}), Zone, <<"get">>, Req, error, #{reason => Reason}),
                    {stop, cowboy_req:reply(Code, #{}, Msg, Req), State}
            end;

        {error, not_found} ->
            {stop, cowboy_req:reply(404, Req), State};
        {error, Reason} ->
            {Code, Msg} = sarc_handler_utils:map_error(Reason),
            sarc_auth:log_action(maps:get(auth_user, State, #{}), Zone, <<"get">>, Req, error, #{reason => Reason}),
            {stop, cowboy_req:reply(Code, #{}, Msg, Req), State}
    end.

delete_resource(Req, State = #{zone_id := Zone, hash_bin := Hash}) ->
    case sarc_port:object_delete(Zone, Hash) of
        ok ->
            sarc_auth:log_action(maps:get(auth_user, State, #{}), Zone, <<"delete">>, Req, ok, #{hash => sarc_handler_utils:hash_to_hex(Hash)}),
            {true, Req, State};
        {error, Reason} ->
            {Code, Msg} = sarc_handler_utils:map_error(Reason),
            sarc_auth:log_action(maps:get(auth_user, State, #{}), Zone, <<"delete">>, Req, error, #{reason => Reason}),
            {false, cowboy_req:reply(Code, #{}, Msg, Req), State}
    end.
