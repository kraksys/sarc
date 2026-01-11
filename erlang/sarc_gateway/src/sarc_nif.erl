%%%-------------------------------------------------------------------
%%% @doc Erlang NIF wrapper for SARC object store (READ operations)
%%%
%%% Provides low-latency access to object store read operations:
%%% - object_get/2: Retrieve object data and metadata
%%% - object_exists/2: Check if object exists
%%% - object_query/3: Query objects with filters
%%%
%%% The NIF library is automatically loaded on module initialization.
%%% @end
%%%-------------------------------------------------------------------
-module(sarc_nif).

%% API exports
-export([
    load/0,
    object_get/2,
    object_get_metadata/2,
    object_exists/2,
    object_query/3,
    object_count/1,
    object_db_info/0
]).

%% NIF stubs (replaced by C++ implementation)
-export([
    object_get_nif/2,
    object_get_metadata_nif/2,
    object_exists_nif/2,
    object_query_nif/3,
    object_count_nif/1,
    object_db_info_nif/0
]).

%% NIF is loaded explicitly by sarc_gateway_app after env setup.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type zone_id() :: sarc_codec:zone_id().
-type hash256() :: sarc_codec:hash256().
-type object_key() :: sarc_codec:object_key().
-type object_meta() :: #{
    size => non_neg_integer(),
    refcount => pos_integer(),
    created_at => integer(),
    updated_at => integer()
}.
-type error_reason() :: not_found | invalid | io_error | corrupted |
                       permission_denied | conflict | unavailable | unknown_error.

-export_type([object_meta/0, error_reason/0]).

%%%===================================================================
%%% API Functions
%%%===================================================================

%% @doc Retrieve object data and metadata
%%
%% Returns the object data as binary along with metadata.
%% This is a high-level wrapper that validates input before calling the NIF.
%%
%% Example:
%%   Hash = <<0:256>>,
%%   {ok, Data, Meta} = sarc_nif:object_get(1, Hash).
-spec object_get(zone_id(), hash256()) ->
    {ok, binary(), object_meta()} | {error, error_reason()}.
object_get(ZoneId, Hash) ->
    case sarc_codec:validate_zone_id(ZoneId) of
        ok ->
            case sarc_codec:validate_hash256(Hash) of
                ok -> object_get_nif(ZoneId, Hash);
                {error, _} -> {error, invalid}
            end;
        {error, _} -> {error, invalid}
    end.

%% @doc Retrieve object metadata (including filesystem path)
-spec object_get_metadata(zone_id(), hash256()) ->
    {ok, object_meta()} | {error, error_reason()}.
object_get_metadata(ZoneId, Hash) ->
    case sarc_codec:validate_zone_id(ZoneId) of
        ok ->
            case sarc_codec:validate_hash256(Hash) of
                ok -> object_get_metadata_nif(ZoneId, Hash);
                {error, _} -> {error, invalid}
            end;
        {error, _} -> {error, invalid}
    end.

%% @doc Check if object exists
%%
%% Fast existence check without retrieving data.
%%
%% Example:
%%   {ok, true} = sarc_nif:object_exists(1, Hash).
-spec object_exists(zone_id(), hash256()) ->
    {ok, boolean()} | {error, error_reason()}.
object_exists(ZoneId, Hash) ->
    case sarc_codec:validate_zone_id(ZoneId) of
        ok ->
            case sarc_codec:validate_hash256(Hash) of
                ok -> object_exists_nif(ZoneId, Hash);
                {error, _} -> {error, invalid}
            end;
        {error, _} -> {error, invalid}
    end.

%% @doc Query objects with filters
%%
%% Filter options:
%%   - min_size: Minimum object size in bytes
%%   - max_size: Maximum object size in bytes
%%   - filename_pattern: SQL LIKE pattern (e.g., "%.txt")
%%   - mime_type: Exact MIME type match
%%
%% Example:
%%   Filter = #{min_size => 1024, max_size => 1048576},
%%   {ok, Keys} = sarc_nif:object_query(1, Filter, 100).
-spec object_query(zone_id(), map(), pos_integer()) ->
    {ok, [object_key()]} | {error, error_reason()}.
object_query(ZoneId, Filter, Limit) when is_integer(Limit), Limit > 0 ->
    case sarc_codec:validate_zone_id(ZoneId) of
        ok ->
            ValidFilter = validate_filter(Filter),
            object_query_nif(ZoneId, ValidFilter, Limit);
        {error, _} -> {error, invalid}
    end;
object_query(_, _, _) ->
    {error, invalid}.

%% @doc Count objects in a zone
-spec object_count(zone_id()) ->
    {ok, non_neg_integer()} | {error, error_reason()}.
object_count(ZoneId) ->
    case sarc_codec:validate_zone_id(ZoneId) of
        ok -> object_count_nif(ZoneId);
        {error, _} -> {error, invalid}
    end.

%% @doc Get DB info used by NIF (debug)
object_db_info() ->
    object_db_info_nif().

%%%===================================================================
%%% NIF Stubs (Replaced by C++ Implementation)
%%%===================================================================

%% @private
object_get_nif(_ZoneId, _Hash) ->
    erlang:nif_error(nif_not_loaded).

%% @private
object_get_metadata_nif(_ZoneId, _Hash) ->
    erlang:nif_error(nif_not_loaded).

%% @private
object_exists_nif(_ZoneId, _Hash) ->
    erlang:nif_error(nif_not_loaded).

%% @private
object_query_nif(_ZoneId, _Filter, _Limit) ->
    erlang:nif_error(nif_not_loaded).

%% @private
object_count_nif(_ZoneId) ->
    erlang:nif_error(nif_not_loaded).

%% @private
object_db_info_nif() ->
    erlang:nif_error(nif_not_loaded).

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%% @private
%% Load NIF library (explicit)
load() ->
    case code:priv_dir(sarc_gateway) of
        {error, bad_name} ->
            % Development mode: try relative path
            SoPath = filename:join(["..", "priv", "sarc_nif"]),
            normalize_load_result(erlang:load_nif(SoPath, load_info()));
        PrivDir ->
            SoPath = filename:join(PrivDir, "sarc_nif"),
            normalize_load_result(erlang:load_nif(SoPath, load_info()))
    end.

normalize_load_result(ok) -> ok;
normalize_load_result({error, {reload, _}}) -> ok;
normalize_load_result({error, {already_loaded, _}}) -> ok;
normalize_load_result(Err) -> Err.

load_info() ->
    DataRoot = os:getenv("SARC_DATA_ROOT"),
    DbPath = os:getenv("SARC_DB_PATH"),
    Map0 = #{},
    Map1 = case DataRoot of
        false -> Map0;
        _ -> Map0#{data_root => list_to_binary(DataRoot)}
    end,
    case DbPath of
        false -> Map1;
        _ -> Map1#{db_path => list_to_binary(DbPath)}
    end.

%% @private
%% Validate and normalize filter map
validate_filter(Filter) when is_map(Filter) ->
    Filter;
validate_filter(_) ->
    #{}.

%%%===================================================================
%%% EUnit Tests
%%%===================================================================

-ifdef(TEST).

%%--------------------------------------------------------------------
%% Validation Tests (NIF not required)
%%--------------------------------------------------------------------

object_get_validation_test() ->
    % Invalid zone
    ?assertEqual({error, invalid}, object_get(0, <<0:256>>)),
    ?assertEqual({error, invalid}, object_get(65536, <<0:256>>)),

    % Invalid hash
    ?assertEqual({error, invalid}, object_get(1, <<>>)),
    ?assertEqual({error, invalid}, object_get(1, <<0:248>>)).

object_exists_validation_test() ->
    % Invalid zone
    ?assertEqual({error, invalid}, object_exists(0, <<0:256>>)),

    % Invalid hash
    ?assertEqual({error, invalid}, object_exists(1, <<>>)).

object_query_validation_test() ->
    % Invalid zone
    ?assertEqual({error, invalid}, object_query(0, #{}, 10)),

    % Invalid limit
    ?assertEqual({error, invalid}, object_query(1, #{}, 0)),
    ?assertEqual({error, invalid}, object_query(1, #{}, -1)).

validate_filter_test() ->
    ?assertEqual(#{}, validate_filter(#{})),
    ?assertEqual(#{min_size => 100}, validate_filter(#{min_size => 100})),
    ?assertEqual(#{}, validate_filter(not_a_map)).

%%--------------------------------------------------------------------
%% NIF Integration Tests (require NIF loaded)
%%--------------------------------------------------------------------

% Note: These tests will fail with nif_not_loaded until the NIF is properly loaded
% They serve as integration tests when running with the full system

-ifdef(NIF_LOADED).

nif_loading_test() ->
    % This test verifies NIF is loaded by attempting to call it
    % It should not crash with nif_not_loaded
    Result = object_exists(1, <<0:256>>),
    ?assertMatch({ok, _}, Result).

-endif.

-endif.
