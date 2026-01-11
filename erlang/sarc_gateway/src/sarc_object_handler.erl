-module(sarc_object_handler).

%% Cowboy REST callbacks
-export([init/2, allowed_methods/2, resource_exists/2, content_types_provided/2,
         delete_resource/2, provide_object/2]).

init(Req, State) ->
    Zone = cowboy_req:binding(zone, Req),
    Hash = cowboy_req:binding(hash, Req),
    {cowboy_rest, Req, State#{zone => Zone, hash => Hash}}.

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
            {stop, Req1, State};

        {ok, _Meta} ->
            %% Small file: read into memory
            case sarc_nif:object_get_nif(Zone, Hash) of
                {ok, Data, _} ->
                    Req1 = cowboy_req:set_resp_header(<<"etag">>,
                                                       sarc_handler_utils:hash_to_hex(Hash), Req),
                    {Data, Req1, State};
                {error, Reason} ->
                    {Code, Msg} = sarc_handler_utils:map_error(Reason),
                    {stop, cowboy_req:reply(Code, #{}, Msg, Req), State}
            end;

        {error, not_found} ->
            {stop, cowboy_req:reply(404, Req), State};
        {error, Reason} ->
            {Code, Msg} = sarc_handler_utils:map_error(Reason),
            {stop, cowboy_req:reply(Code, #{}, Msg, Req), State}
    end.

delete_resource(Req, State = #{zone_id := Zone, hash_bin := Hash}) ->
    case sarc_port:object_delete(Zone, Hash) of
        ok ->
            {true, Req, State};
        {error, Reason} ->
            {Code, Msg} = sarc_handler_utils:map_error(Reason),
            {false, cowboy_req:reply(Code, #{}, Msg, Req), State}
    end.
