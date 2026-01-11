-module(sarc_http_router).

%% API
-export([routes/0]).

%%====================================================================
%% API
%%====================================================================

routes() ->
    [
        %% Health check endpoint
        {"/health", sarc_health_handler, #{}},

        %% Object operations
        {"/objects", sarc_upload_handler, #{}},                      % PUT
        {"/objects/:zone/:hash", sarc_object_handler, #{}},          % GET, DELETE

        %% Query operations
        {"/zones/:zone/objects", sarc_query_handler, #{}},           % GET

        %% Garbage collection
        {"/gc/:zone", sarc_gc_handler, #{}},                         % POST

        %% WebSocket endpoint
        {"/ws", sarc_ws_handler, #{}}
    ].

%%====================================================================
%% Internal functions
%%====================================================================
