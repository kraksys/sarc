-module(sarc_handler_utils).

%% Utility exports
-export([
    read_body/1,
    parse_zone/1,
    parse_zone_and_hash/2,
    build_filter/1,
    parse_limit/1,
    format_key/1,
    format_meta/1,
    hash_to_hex/1,
    map_error/1,
    error_response/3,
    json_response/2
]).

%%====================================================================
%% Utility functions
%%====================================================================

%% Read request body (with size limit)
read_body(Req) ->
    read_body(Req, <<>>).

read_body(Req0, Acc) ->
    case cowboy_req:read_body(Req0, #{length => 1000000, period => 5000}) of
        {ok, Data, Req} ->
            {ok, <<Acc/binary, Data/binary>>, Req};
        {more, Data, Req} ->
            read_body(Req, <<Acc/binary, Data/binary>>)
    end.

%% Parse zone ID from binary
parse_zone(Bin) when is_binary(Bin) ->
    try
        Zone = binary_to_integer(Bin),
        if
            Zone >= 1 andalso Zone =< 65535 ->
                {ok, Zone};
            true ->
                {error, invalid_zone}
        end
    catch
        _:_ ->
            {error, invalid_zone}
    end;
parse_zone(_) ->
    {error, invalid_zone}.

%% Parse zone and hash
parse_zone_and_hash(ZoneBin, HashHex) ->
    case parse_zone(ZoneBin) of
        {ok, Zone} ->
            case sarc_codec:hex_to_binary(HashHex) of
                {ok, Hash} ->
                    case sarc_codec:validate_hash256(Hash) of
                        ok -> {ok, Zone, Hash};
                        Error -> Error
                    end;
                Error -> Error
            end;
        Error ->
            Error
    end.

%% Build filter map from query string
build_filter(QsVals) ->
    Filter = #{},
    Filter1 = case proplists:get_value(<<"filename_pattern">>, QsVals) of
        undefined -> Filter;
        Pattern -> Filter#{filename_pattern => Pattern}
    end,
    Filter2 = case proplists:get_value(<<"mime_type">>, QsVals) of
        undefined -> Filter1;
        Mime -> Filter1#{mime_type => Mime}
    end,
    Filter3 = case proplists:get_value(<<"min_size">>, QsVals) of
        undefined -> Filter2;
        MinBin ->
            try
                Filter2#{min_size => binary_to_integer(MinBin)}
            catch _:_ -> Filter2
            end
    end,
    Filter4 = case proplists:get_value(<<"max_size">>, QsVals) of
        undefined -> Filter3;
        MaxBin ->
            try
                Filter3#{max_size => binary_to_integer(MaxBin)}
            catch _:_ -> Filter3
            end
    end,
    Filter4.

%% Parse limit parameter
parse_limit(Bin) when is_binary(Bin) ->
    try
        L = binary_to_integer(Bin),
        min(L, 1000)  % Max 1000 results
    catch
        _:_ -> 100
    end;
parse_limit(_) ->
    100.

%% Format object key for JSON
format_key({Zone, Hash}) ->
    #{
        zone => Zone,
        hash => hash_to_hex(Hash)
    };
format_key(Map) when is_map(Map) ->
    Base = #{
        zone => maps:get(zone, Map),
        hash => hash_to_hex(maps:get(hash, Map)),
        size => maps:get(size, Map, 0),
        created_at => maps:get(created_at, Map, 0)
    },
    Base1 = case maps:get(filename, Map, undefined) of
        undefined -> Base;
        Filename -> Base#{filename => Filename}
    end,
    case maps:get(mime_type, Map, undefined) of
        undefined -> Base1;
        Mime -> Base1#{mime_type => Mime}
    end.

%% Format metadata for JSON
format_meta(Meta) ->
    Base = #{
        size => maps:get(size, Meta, 0),
        refcount => maps:get(refcount, Meta, 0),
        created_at => maps:get(created_at, Meta, 0),
        updated_at => maps:get(updated_at, Meta, 0)
    },
    Base1 = case maps:get(filename, Meta, undefined) of
        undefined -> Base;
        Filename -> Base#{filename => Filename}
    end,
    case maps:get(mime_type, Meta, undefined) of
        undefined -> Base1;
        Mime -> Base1#{mime_type => Mime}
    end.

%% Convert hash binary to hex string
hash_to_hex(Hash) when is_binary(Hash) ->
    list_to_binary(sarc_codec:binary_to_hex(Hash)).

%% Map error codes to HTTP status codes
map_error(not_found) -> {404, <<"Not Found">>};
map_error(invalid) -> {400, <<"Bad Request">>};
map_error(invalid_handle) -> {400, <<"Bad Request">>};
map_error(io) -> {500, <<"Internal Server Error">>};
map_error(io_error) -> {500, <<"Internal Server Error">>};
map_error(port_died) -> {502, <<"Bad Gateway">>};
map_error(corrupt) -> {500, <<"Data Corruption">>};
map_error(permission_denied) -> {403, <<"Forbidden">>};
map_error(conflict) -> {409, <<"Conflict">>};
map_error(unavailable) -> {503, <<"Service Unavailable">>};
map_error(out_of_memory) -> {507, <<"Insufficient Storage">>};
map_error(_) -> {500, <<"Internal Server Error">>}.

%% Create error response (replies immediately)
error_response(Req, Code, Message) ->
    Body = jsx:encode(#{error => Message}),
    cowboy_req:reply(Code, #{
        <<"content-type">> => <<"application/json">>
    }, Body, Req).

%% JSON response helper
json_response(Req, Data) ->
    Json = jsx:encode(Data),
    cowboy_req:reply(200, #{
        <<"content-type">> => <<"application/json">>
    }, Json, Req).
