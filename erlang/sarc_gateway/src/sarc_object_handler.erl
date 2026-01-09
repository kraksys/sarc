-module(sarc_object_handler).

%% Cowboy REST callbacks
-export([init/2, allowed_methods/2, resource_exists/2, content_types_provided/2,
         delete_resource/2, object_to_binary/2]).

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
        {{<<"application">>, <<"octet-stream">>, '*'}, object_to_binary}
    ], Req, State}.

object_to_binary(Req, State = #{zone_id := Zone, hash_bin := Hash}) ->
    case sarc_nif:object_get_nif(Zone, Hash) of
        {ok, Data, _Meta} ->
            %% Set appropriate headers
            Req1 = cowboy_req:set_resp_header(<<"content-length">>,
                                               integer_to_binary(byte_size(Data)), Req),
            Req2 = cowboy_req:set_resp_header(<<"etag">>,
                                               sarc_handler_utils:hash_to_hex(Hash), Req1),
            {Data, Req2, State};
        {error, not_found} ->
            {halt, cowboy_req:reply(404, Req), State};
        {error, Reason} ->
            {Code, Msg} = sarc_handler_utils:map_error(Reason),
            {halt, cowboy_req:reply(Code, #{}, Msg, Req), State}
    end.

delete_resource(Req, State = #{zone_id := Zone, hash_bin := Hash}) ->
    case sarc_port:object_delete(Zone, Hash) of
        ok ->
            {true, Req, State};
        {error, Reason} ->
            {Code, Msg} = sarc_handler_utils:map_error(Reason),
            {false, cowboy_req:reply(Code, #{}, Msg, Req), State}
    end.
