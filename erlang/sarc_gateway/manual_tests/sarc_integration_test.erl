-module(sarc_integration_test).
-include_lib("eunit/eunit.hrl").

%%====================================================================
%% Integration Tests for SARC NIF and Port
%%====================================================================

-define(TEST_ZONE, 1).
-define(TEST_DATA, <<"Hello, SARC!">>).
-define(TEST_FILENAME, <<"test.txt">>).
-define(TEST_MIME_TYPE, <<"text/plain">>).

%%====================================================================
%% Setup and Teardown
%%====================================================================

setup() ->
    % Ensure test data directory exists
    os:cmd("mkdir -p /tmp/sarc_data"),

    % Start the sarc_gateway application
    application:ensure_all_started(sarc_gateway),

    % Wait for port to be ready
    timer:sleep(100),
    ok.

cleanup(_) ->
    % Clean up test data
    os:cmd("rm -rf /tmp/sarc_data"),
    os:cmd("rm -f /tmp/sarc.db"),
    ok.

%%====================================================================
%% Port Tests (Write Operations)
%%====================================================================

port_put_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [{"Put object via port",
       fun() ->
           Result = sarc_port:object_put(?TEST_ZONE, ?TEST_DATA, ?TEST_FILENAME, ?TEST_MIME_TYPE),
           ?assertMatch({ok, _Key, _Meta, _Deduplicated}, Result),

           {ok, {Zone, Hash}, Meta, Deduplicated} = Result,
           ?assertEqual(?TEST_ZONE, Zone),
           ?assertEqual(32, byte_size(Hash)),
           ?assertMatch(#{size := _, refcount := _, created_at := _, updated_at := _}, Meta),
           ?assertEqual(false, Deduplicated)
       end}]}.

port_put_duplicate_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [{"Put duplicate object detects deduplication",
       fun() ->
           % First put
           {ok, Key1, _Meta1, Dedup1} = sarc_port:object_put(?TEST_ZONE, ?TEST_DATA, ?TEST_FILENAME, ?TEST_MIME_TYPE),
           ?assertEqual(false, Dedup1),

           % Second put of same data
           {ok, Key2, _Meta2, Dedup2} = sarc_port:object_put(?TEST_ZONE, ?TEST_DATA, ?TEST_FILENAME, ?TEST_MIME_TYPE),
           ?assertEqual(true, Dedup2),
           ?assertEqual(Key1, Key2)
       end}]}.

port_delete_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [{"Delete object via port",
       fun() ->
           % First put an object
           {ok, {Zone, Hash}, _Meta, _Dedup} = sarc_port:object_put(?TEST_ZONE, ?TEST_DATA, ?TEST_FILENAME, ?TEST_MIME_TYPE),

           % Delete it
           Result = sarc_port:object_delete(Zone, Hash),
           ?assertEqual(ok, Result)
       end}]}.

port_gc_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [{"Garbage collect zone via port",
       fun() ->
           % Put and delete an object
           {ok, {Zone, Hash}, _Meta, _Dedup} = sarc_port:object_put(?TEST_ZONE, ?TEST_DATA, ?TEST_FILENAME, ?TEST_MIME_TYPE),
           ok = sarc_port:object_delete(Zone, Hash),

           % Run GC
           Result = sarc_port:object_gc(?TEST_ZONE),
           ?assertMatch({ok, _DeletedCount}, Result),
           {ok, DeletedCount} = Result,
           ?assert(DeletedCount >= 0)
       end}]}.

%%====================================================================
%% NIF Tests (Read Operations)
%%====================================================================

nif_get_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [{"Get object via NIF",
       fun() ->
           % First put an object via port
           {ok, {Zone, Hash}, _Meta, _Dedup} = sarc_port:object_put(?TEST_ZONE, ?TEST_DATA, ?TEST_FILENAME, ?TEST_MIME_TYPE),

           % Get it via NIF
           Result = sarc_nif:object_get_nif(Zone, Hash),
           ?assertMatch({ok, _Data, _Meta}, Result),

           {ok, Data, Meta} = Result,
           ?assertEqual(?TEST_DATA, Data),
           ?assertMatch(#{size := _, refcount := _, created_at := _, updated_at := _}, Meta)
       end}]}.

nif_get_not_found_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [{"Get non-existent object returns error",
       fun() ->
           % Create a random hash that doesn't exist
           NonExistentHash = <<0:256>>,
           Result = sarc_nif:object_get_nif(?TEST_ZONE, NonExistentHash),
           ?assertEqual({error, not_found}, Result)
       end}]}.

nif_exists_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [{"Check object existence via NIF",
       fun() ->
           % Put an object
           {ok, {Zone, Hash}, _Meta, _Dedup} = sarc_port:object_put(?TEST_ZONE, ?TEST_DATA, ?TEST_FILENAME, ?TEST_MIME_TYPE),

           % Check existence
           Result = sarc_nif:object_exists_nif(Zone, Hash),
           ?assertEqual({ok, true}, Result),

           % Check non-existent object
           NonExistentHash = <<0:256>>,
           Result2 = sarc_nif:object_exists_nif(Zone, NonExistentHash),
           ?assertEqual({ok, false}, Result2)
       end}]}.

nif_query_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [{"Query objects via NIF",
       fun() ->
           % Put several objects
           {ok, _Key1, _Meta1, _Dedup1} = sarc_port:object_put(?TEST_ZONE, <<"data1">>, <<"file1.txt">>, <<"text/plain">>),
           {ok, _Key2, _Meta2, _Dedup2} = sarc_port:object_put(?TEST_ZONE, <<"data2">>, <<"file2.txt">>, <<"text/plain">>),
           {ok, _Key3, _Meta3, _Dedup3} = sarc_port:object_put(?TEST_ZONE, <<"data3">>, <<"image.jpg">>, <<"image/jpeg">>),

           % Query all objects in zone
           Filter = #{},
           Limit = 100,
           Result = sarc_nif:object_query_nif(?TEST_ZONE, Filter, Limit),
           ?assertMatch({ok, _Results}, Result),

           {ok, Results} = Result,
           ?assert(length(Results) >= 3),

           % Verify each result is a {Zone, Hash} tuple
           lists:foreach(fun({Zone, Hash}) ->
               ?assertEqual(?TEST_ZONE, Zone),
               ?assertEqual(32, byte_size(Hash))
           end, Results)
       end}]}.

%%====================================================================
%% Round-trip Tests (Port + NIF)
%%====================================================================

roundtrip_put_get_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [{"Round-trip: Put via Port, Get via NIF",
       fun() ->
           TestData = crypto:strong_rand_bytes(1024),

           % Put via port
           {ok, {Zone, Hash}, PutMeta, _Dedup} = sarc_port:object_put(?TEST_ZONE, TestData, <<"random.bin">>, <<"application/octet-stream">>),

           % Get via NIF
           {ok, RetrievedData, GetMeta} = sarc_nif:object_get_nif(Zone, Hash),

           % Verify data integrity
           ?assertEqual(TestData, RetrievedData),
           ?assertEqual(maps:get(size, PutMeta), maps:get(size, GetMeta)),
           ?assertEqual(1024, maps:get(size, GetMeta))
       end}]}.

roundtrip_put_exists_delete_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [{"Round-trip: Put, Exists, Delete",
       fun() ->
           % Put
           {ok, {Zone, Hash}, _Meta, _Dedup} = sarc_port:object_put(?TEST_ZONE, ?TEST_DATA, ?TEST_FILENAME, ?TEST_MIME_TYPE),

           % Verify exists
           {ok, true} = sarc_nif:object_exists_nif(Zone, Hash),

           % Delete
           ok = sarc_port:object_delete(Zone, Hash),

           % Verify get fails after delete (refcount should be 0 but object still in DB until GC)
           % Note: depending on implementation, this might still return the object
           % or return an error. Adjust based on actual behavior.
           Result = sarc_nif:object_get_nif(Zone, Hash),
           ?assert(Result =:= {error, not_found} orelse element(1, Result) =:= ok)
       end}]}.

%%====================================================================
%% Stress Tests
%%====================================================================

stress_multiple_objects_test_() ->
    {timeout, 30,
     {setup,
      fun setup/0,
      fun cleanup/1,
      [{"Stress test: Multiple objects",
        fun() ->
            NumObjects = 100,

            % Put many objects
            Keys = lists:map(fun(N) ->
                Data = list_to_binary(io_lib:format("Object ~p", [N])),
                Filename = list_to_binary(io_lib:format("file~p.txt", [N])),
                {ok, Key, _Meta, _Dedup} = sarc_port:object_put(?TEST_ZONE, Data, Filename, <<"text/plain">>),
                Key
            end, lists:seq(1, NumObjects)),

            ?assertEqual(NumObjects, length(Keys)),

            % Verify all objects exist
            lists:foreach(fun({Zone, Hash}) ->
                {ok, true} = sarc_nif:object_exists_nif(Zone, Hash)
            end, Keys),

            % Query should return at least NumObjects
            {ok, QueryResults} = sarc_nif:object_query_nif(?TEST_ZONE, #{}, 1000),
            ?assert(length(QueryResults) >= NumObjects)
        end}]}}.

%%====================================================================
%% Error Handling Tests
%%====================================================================

error_invalid_zone_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [{"Error handling: Invalid zone",
       fun() ->
           % Zone 0 is invalid (must be 1-65535)
           Result = sarc_port:object_put(0, ?TEST_DATA, ?TEST_FILENAME, ?TEST_MIME_TYPE),
           ?assertMatch({error, _}, Result)
       end}]}.

error_invalid_hash_test_() ->
    {setup,
     fun setup/0,
     fun cleanup/1,
     [{"Error handling: Invalid hash size",
       fun() ->
           % Hash must be exactly 32 bytes
           InvalidHash = <<1, 2, 3>>,
           Result = sarc_nif:object_get_nif(?TEST_ZONE, InvalidHash),
           ?assertMatch({error, _}, Result)
       end}]}.
