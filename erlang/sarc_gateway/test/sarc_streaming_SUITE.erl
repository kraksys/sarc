-module(sarc_streaming_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([streaming_upload_download_test/1]).

all() -> [streaming_upload_download_test].

get_data_root() ->
    "/tmp/sarc_data".

get_db_path() ->
    "/tmp/sarc.db".

pick_zone() ->
    (erlang:unique_integer([positive]) rem 65534) + 1.

wait_for_port(0, _DelayMs) ->
    {error, timeout};
wait_for_port(Retries, DelayMs) ->
    try ranch:get_port(sarc_http_listener) of
        Port when is_integer(Port), Port > 0 ->
            {ok, Port};
        _ ->
            timer:sleep(DelayMs),
            wait_for_port(Retries - 1, DelayMs)
    catch
        _:_ ->
            timer:sleep(DelayMs),
            wait_for_port(Retries - 1, DelayMs)
    end.

wait_for_health(_Port, 0) ->
    {error, timeout};
wait_for_health(Port, Retries) ->
    Url = "http://localhost:" ++ integer_to_list(Port) ++ "/health",
    case httpc:request(Url) of
        {ok, {{_, 200, _}, _, _Body}} ->
            ok;
        _ ->
            timer:sleep(100),
            wait_for_health(Port, Retries - 1)
    end.

init_per_suite(Config) ->
    %% Fixed paths to keep NIF/port aligned even if NIF was loaded earlier
    DataRoot = get_data_root(),
    DbPath = get_db_path(),

    %% Set environment variables explicitly
    os:putenv("SARC_DATA_ROOT", DataRoot),
    os:putenv("SARC_DB_PATH", DbPath),
    os:putenv("SARC_HTTP_PORT", "0"),

    ok = case application:load(sarc_gateway) of
        ok -> ok;
        {error, {already_loaded, sarc_gateway}} -> ok
    end,
    ok = application:set_env(sarc_gateway, http_port, 0),

    {ok, _} = application:ensure_all_started(inets),
    {ok, _} = application:ensure_all_started(sarc_gateway),
    {ok, Port} = wait_for_port(20, 100),
    ok = wait_for_health(Port, 20),
    Zone = pick_zone(),
    [{port, Port}, {zone, Zone} | Config].

end_per_suite(_Config) ->
    application:stop(sarc_gateway),
    application:stop(cowboy),
    application:stop(ranch),
    application:stop(inets),
    ok.

streaming_upload_download_test(_Config) ->
    Port = proplists:get_value(port, _Config, 8080),
    Zone = proplists:get_value(zone, _Config, 1),
    %% Create a 2MB binary
    Size = 2 * 1024 * 1024,
    Data = crypto:strong_rand_bytes(Size),
    
    %% Streaming upload via PUT (sarc_upload_handler)
    Url = "http://localhost:" ++ integer_to_list(Port) ++ "/objects?zone=" ++ integer_to_list(Zone) ++
        "&filename=streaming.bin",
    
    {ok, {{_, 200, _}, _, PutBody}} = httpc:request(put, {Url, [], "application/octet-stream", Data}, [], []),
    PutJson = jsx:decode(list_to_binary(PutBody), [return_maps]),
    
    KeyMap = maps:get(<<"key">>, PutJson),
    HashHex = maps:get(<<"hash">>, KeyMap),
    HashStr = binary_to_list(HashHex),
    
    %% GET - should trigger streaming path (> 1MB)
    GetUrl = "http://localhost:" ++ integer_to_list(Port) ++ "/objects/" ++ integer_to_list(Zone) ++ "/" ++ HashStr,
    {ok, {{_, 200, _}, _, GetBody}} = httpc:request(GetUrl),
    ?assertEqual(Data, list_to_binary(GetBody)),
    
    %% Verify metadata
    {ok, {{_, 200, _}, _, MetaBody}} = httpc:request(GetUrl),
    ?assertEqual(Size, byte_size(list_to_binary(MetaBody))).
