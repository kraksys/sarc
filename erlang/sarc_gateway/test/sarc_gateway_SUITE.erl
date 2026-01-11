-module(sarc_gateway_SUITE).
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([health_check_test/1, object_lifecycle_test/1, query_test/1]).

all() -> [health_check_test, object_lifecycle_test, query_test].

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

    %% Set environment variables BEFORE starting app/loading NIF
    os:putenv("SARC_DATA_ROOT", DataRoot),
    os:putenv("SARC_DB_PATH", DbPath),
    %% Let Cowboy pick an available port (avoid collisions)
    os:putenv("SARC_HTTP_PORT", "0"),

    ok = case application:load(sarc_gateway) of
        ok -> ok;
        {error, {already_loaded, sarc_gateway}} -> ok
    end,
    ok = application:set_env(sarc_gateway, http_port, 0),

    %% Ensure dependencies are started
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

health_check_test(_Config) ->
    Port = proplists:get_value(port, _Config, 8080),
    Url = "http://localhost:" ++ integer_to_list(Port) ++ "/health",
    {ok, {{_, 200, _}, _, Body}} = httpc:request(Url),
    Json = jsx:decode(list_to_binary(Body), [return_maps]),
    ?assertEqual(<<"ok">>, maps:get(<<"status">>, Json)),
    ?assertEqual(<<"0.1.0">>, maps:get(<<"version">>, Json)).

object_lifecycle_test(_Config) ->
    Port = proplists:get_value(port, _Config, 8080),
    Zone = proplists:get_value(zone, _Config, 1),
    Data = <<"integration_test_data">>,
    %% Use PUT for upload
    Url = "http://localhost:" ++ integer_to_list(Port) ++ "/objects?zone=" ++ integer_to_list(Zone) ++
        "&filename=test.txt&mime_type=text/plain",
    
    {ok, {{_, 200, _}, _, PutBody}} = httpc:request(put, {Url, [], "application/octet-stream", Data}, [], []),
    PutJson = jsx:decode(list_to_binary(PutBody), [return_maps]),
    
    KeyMap = maps:get(<<"key">>, PutJson),
    HashHex = maps:get(<<"hash">>, KeyMap),
    HashStr = if is_binary(HashHex) -> binary_to_list(HashHex); true -> HashHex end,
    
    %% GET
    GetUrl = "http://localhost:" ++ integer_to_list(Port) ++ "/objects/" ++ integer_to_list(Zone) ++ "/" ++ HashStr,
    {ok, {{_, 200, _}, _, GetBody}} = httpc:request(GetUrl),
    ?assertEqual(Data, list_to_binary(GetBody)),
    
    %% DELETE
    {ok, {{_, Code, _}, _, _}} = httpc:request(delete, {GetUrl, []}, [], []),
    ?assert(Code =:= 204 orelse Code =:= 200),
    
    %% GET (should fail now with 404)
    {ok, {{_, 404, _}, _, _}} = httpc:request(GetUrl).

query_test(_Config) ->
    Port = proplists:get_value(port, _Config, 8080),
    Zone = proplists:get_value(zone, _Config, 2),
    %% Upload a few objects to zone 2
    Data1 = <<"query_data_1">>,
    Data2 = <<"query_data_2">>,
    Url = "http://localhost:" ++ integer_to_list(Port) ++ "/objects?zone=" ++ integer_to_list(Zone),
    
    httpc:request(put, {Url, [], "application/octet-stream", Data1}, [], []),
    httpc:request(put, {Url, [], "application/octet-stream", Data2}, [], []),
    
    %% Sleep to ensure consistency
    timer:sleep(500),

    %% Query
    QueryUrl = "http://localhost:" ++ integer_to_list(Port) ++ "/zones/" ++ integer_to_list(Zone) ++ "/objects",
    {ok, {{_, 200, _}, _, Body}} = httpc:request(QueryUrl),
    Json = jsx:decode(list_to_binary(Body), [return_maps]),
    
    Results = maps:get(<<"results">>, Json),
    Count = maps:get(<<"count">>, Json),
    
    ?assert(is_list(Results)),
    ?assert(Count >= 2).
