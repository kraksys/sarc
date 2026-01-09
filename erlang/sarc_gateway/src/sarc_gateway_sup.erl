-module(sarc_gateway_sup).
-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 5,
        period => 10
    },

    %% Child specifications
    ChildSpecs = [
        %% sarc_port gen_server for write operations
        #{
            id => sarc_port,
            start => {sarc_port, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [sarc_port]
        }
    ],

    {ok, {SupFlags, ChildSpecs}}.

%%====================================================================
%% Internal functions
%%====================================================================
