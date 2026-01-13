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

        %% Auth endpoints
        {"/auth/init", sarc_auth_init_handler, #{}},
        {"/auth/pair/request", sarc_auth_pair_request_handler, #{}},
        {"/auth/pair/approve", sarc_auth_pair_approve_handler, #{}},

        %% Object operations
        {"/objects", sarc_upload_handler, #{}},                      % PUT
        {"/objects/:zone/:hash", sarc_object_handler, #{}},          % GET, DELETE

        %% Query operations
        {"/zones/:zone/objects", sarc_query_handler, #{}},           % GET

        %% Zone admin operations
        {"/zones/:zone/share", sarc_zone_share_handler, #{}},        % POST
        {"/zones/:zone/members", sarc_zone_members_handler, #{}},    % GET
        {"/zones/:zone/audit", sarc_zone_audit_handler, #{}},        % GET

        %% Garbage collection
        {"/gc/:zone", sarc_gc_handler, #{}},                         % POST

        %% WebSocket endpoint
        {"/ws", sarc_ws_handler, #{}}
    ].

%%====================================================================
%% Internal functions
%%====================================================================
