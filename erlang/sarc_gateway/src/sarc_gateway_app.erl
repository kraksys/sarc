-module(sarc_gateway_app).
-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

-define(DEFAULT_HTTP_PORT, 8080).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
    %% Setup environment variables for NIF and Port
    ok = setup_env(),

    %% Initialize auth subsystem
    ok = sarc_auth:init(),

    %% Load NIF library
    ok = ensure_nif_loaded(),

    %% Initialize object store via Port
    ok = ensure_port_initialized(),

    %% Get configuration
    HttpPort = get_http_port(),

    %% Start Cowboy HTTP listener
    ok = start_http_listener(HttpPort),

    %% Start pg scope for pubsub
    case pg:start_link(sarc_pubsub) of
        {ok, _} -> ok;
        {error, {already_started, _}} -> ok;
        PgError -> error(PgError)
    end,

    %% Start supervisor tree
    case sarc_gateway_sup:start_link() of
        {ok, Pid} ->
            io:format("SARC Gateway started on port ~p~n", [HttpPort]),
            {ok, Pid};
        Error ->
            Error
    end.

stop(_State) ->
    %% Stop Cowboy listener
    ok = cowboy:stop_listener(sarc_http_listener),
    ok = sarc_auth:close(),
    io:format("SARC Gateway stopped~n"),
    ok.

%%====================================================================
%% Internal functions
%%====================================================================

setup_env() ->
    Home0 = os:getenv("HOME"),
    Home = case Home0 of
        false -> "/tmp";
        V -> V
    end,
    DefaultDataRoot = filename:join(Home, "sarc"),
    DefaultDbPath = filename:join(DefaultDataRoot, "sarc.db"),

    %% Set SARC_DATA_ROOT if not present
    case os:getenv("SARC_DATA_ROOT") of
        false -> 
            os:putenv("SARC_DATA_ROOT", DefaultDataRoot),
            ok;
        _ -> ok
    end,

    %% Set SARC_DB_PATH if not present
    case os:getenv("SARC_DB_PATH") of
        false -> os:putenv("SARC_DB_PATH", DefaultDbPath);
        _ -> ok
    end,

    %% Ensure directories exist (even if env vars were already set by caller/test)
    DataRoot = os:getenv("SARC_DATA_ROOT"),
    DbPath = os:getenv("SARC_DB_PATH"),
    filelib:ensure_dir(filename:join(DataRoot, "anyfile")),
    filelib:ensure_dir(DbPath),
    io:format("SARC env: data_root=~s db_path=~s~n", [DataRoot, DbPath]),
    ok.

get_http_port() ->
    %% Prefer OS env override (useful for tests / multi-instance dev)
    case os:getenv("SARC_HTTP_PORT") of
        false ->
            application:get_env(sarc_gateway, http_port, ?DEFAULT_HTTP_PORT);
        PortStr ->
            try
                list_to_integer(PortStr)
            catch
                _:_ -> application:get_env(sarc_gateway, http_port, ?DEFAULT_HTTP_PORT)
            end
    end.

ensure_nif_loaded() ->
    %% Load NIF explicitly after env setup
    ok = case sarc_nif:load() of
        ok -> ok;
        {error, Reason} ->
            io:format("Warning: NIF load failed: ~p~n", [Reason]),
            ok
    end,
    %% Verify it's available by attempting a simple call
    try
        %% Try a harmless existence check - this will only work if NIF is loaded
        case sarc_nif:object_exists_nif(1, <<0:256>>) of
            {ok, _} ->
                io:format("NIF loaded successfully~n"),
                ok;
            {error, _} ->
                io:format("NIF loaded successfully~n"),
                ok
        end
    catch
        error:nif_not_loaded ->
            io:format("Warning: NIF not loaded, some operations may fail~n"),
            ok;  % Continue anyway for development
        _:_ ->
            io:format("Warning: NIF check failed, some operations may fail~n"),
            ok
    end.

ensure_port_initialized() ->
    %% Port will be started by supervisor
    %% This is just a placeholder for future initialization
    ok.

start_http_listener(Port) ->
    %% Define routes using sarc_http_router
    Dispatch = cowboy_router:compile([
        {'_', sarc_http_router:routes()}
    ]),

    %% Start Cowboy HTTP listener
    {ok, _} = cowboy:start_clear(
        sarc_http_listener,
        [{port, Port}],
        #{
            env => #{dispatch => Dispatch},
            middlewares => [cowboy_router, cowboy_handler]
        }
    ),
    ok.
