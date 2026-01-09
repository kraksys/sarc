-module(sarc_health_handler).

%% Cowboy REST callbacks
-export([init/2, allowed_methods/2, content_types_provided/2, health_to_json/2]).

init(Req, State) ->
    {cowboy_rest, Req, State}.

allowed_methods(Req, State) ->
    {[<<"GET">>], Req, State}.

content_types_provided(Req, State) ->
    {[
        {{<<"application">>, <<"json">>, '*'}, health_to_json}
    ], Req, State}.

health_to_json(Req, State) ->
    Response = #{
        status => ok,
        version => <<"0.1.0">>,
        timestamp => erlang:system_time(millisecond)
    },
    Json = jsx:encode(Response),
    {Json, Req, State}.
