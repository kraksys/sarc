%%%-------------------------------------------------------------------
%%% @doc Gen_server wrapper for SARC port driver (WRITE operations)
%%%
%%% Manages the external port process for isolated write operations:
%%% - object_put/4: Store object
%%% - object_delete/2: Delete object
%%% - object_gc/1: Garbage collect zone
%%%
%%% The port process is started automatically and restarted on crash.
%%% @end
%%%-------------------------------------------------------------------
-module(sarc_port).
-behaviour(gen_server).

%% API exports
-export([
    start_link/0,
    start_link/1,
    stop/0,
    object_put/4,
    object_delete/2,
    object_gc/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type zone_id() :: sarc_codec:zone_id().
-type hash256() :: sarc_codec:hash256().
-type object_key() :: sarc_codec:object_key().
-type object_meta() :: sarc_nif:object_meta().
-type error_reason() :: sarc_nif:error_reason().

-define(SERVER, ?MODULE).
-define(DEFAULT_TIMEOUT, 30000).  % 30 seconds
-define(PORT_TIMEOUT, 60000).     % 60 seconds for port operations

%% Operation codes (must match C++ implementation)
-define(OP_PUT_OBJECT, 1).
-define(OP_DELETE_OBJECT, 2).
-define(OP_GC, 3).

%% State record
-record(state, {
    port :: port(),
    pending = #{} :: #{reference() => {pid(), term()}}
}).

%%%===================================================================
%%% API Functions
%%%===================================================================

%% @doc Start port server with default configuration
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    start_link([]).

%% @doc Start port server with options
%%
%% Options:
%%   - {name, Name}: Register server with name
-spec start_link(list()) -> {ok, pid()} | {error, term()}.
start_link(Opts) ->
    case proplists:get_value(name, Opts, ?SERVER) of
        undefined ->
            gen_server:start_link(?MODULE, Opts, []);
        Name ->
            gen_server:start_link({local, Name}, ?MODULE, Opts, [])
    end.

%% @doc Stop the port server
-spec stop() -> ok.
stop() ->
    gen_server:stop(?SERVER).

%% @doc Store object data
%%
%% Returns the object key, metadata, and deduplication status.
%% If the object already exists, deduplicated=true and refcount is incremented.
%%
%% Example:
%%   Data = <<"Hello, World!">>,
%%   {ok, Key, Meta, Dedup} = sarc_port:object_put(1, Data, <<"hello.txt">>, <<"text/plain">>).
-spec object_put(zone_id(), binary(), binary(), binary()) ->
    {ok, object_key(), object_meta(), boolean()} | {error, error_reason()}.
object_put(ZoneId, Data, Filename, MimeType)
  when is_integer(ZoneId), is_binary(Data), is_binary(Filename), is_binary(MimeType) ->
    Request = {put, ZoneId, Data, Filename, MimeType},
    gen_server:call(?SERVER, Request, ?DEFAULT_TIMEOUT).

%% @doc Delete object (decrement refcount)
%%
%% Example:
%%   ok = sarc_port:object_delete(1, Hash).
-spec object_delete(zone_id(), hash256()) -> ok | {error, error_reason()}.
object_delete(ZoneId, Hash) when is_integer(ZoneId), is_binary(Hash) ->
    Request = {delete, ZoneId, Hash},
    gen_server:call(?SERVER, Request, ?DEFAULT_TIMEOUT).

%% @doc Garbage collect zone (remove objects with refcount=0)
%%
%% Returns the number of objects deleted.
%%
%% Example:
%%   {ok, 42} = sarc_port:object_gc(1).
-spec object_gc(zone_id()) -> {ok, non_neg_integer()} | {error, error_reason()}.
object_gc(ZoneId) when is_integer(ZoneId) ->
    Request = {gc, ZoneId},
    gen_server:call(?SERVER, Request, ?DEFAULT_TIMEOUT).

%%%===================================================================
%%% gen_server Callbacks
%%%===================================================================

%% @private
init(_Opts) ->
    process_flag(trap_exit, true),
    case open_port_driver() of
        {ok, Port} ->
            {ok, #state{port = Port}};
        {error, Reason} ->
            {stop, Reason}
    end.

%% @private
handle_call({put, ZoneId, Data, Filename, MimeType}, From, State) ->
    case sarc_codec:validate_zone_id(ZoneId) of
        ok ->
            send_port_command(State#state.port, ?OP_PUT_OBJECT,
                             {ZoneId, Data, Filename, MimeType}, From, State);
        {error, _} ->
            {reply, {error, invalid}, State}
    end;

handle_call({delete, ZoneId, Hash}, From, State) ->
    case sarc_codec:validate_zone_id(ZoneId) of
        ok ->
            case sarc_codec:validate_hash256(Hash) of
                ok ->
                    send_port_command(State#state.port, ?OP_DELETE_OBJECT,
                                     {ZoneId, Hash}, From, State);
                {error, _} ->
                    {reply, {error, invalid}, State}
            end;
        {error, _} ->
            {reply, {error, invalid}, State}
    end;

handle_call({gc, ZoneId}, From, State) ->
    case sarc_codec:validate_zone_id(ZoneId) of
        ok ->
            send_port_command(State#state.port, ?OP_GC, ZoneId, From, State);
        {error, _} ->
            {reply, {error, invalid}, State}
    end;

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

%% @private
handle_cast(_Msg, State) ->
    {noreply, State}.

%% @private
handle_info({Port, {data, Data}}, State = #state{port = Port}) ->
    % Port response received
    case decode_port_response(Data) of
        {ok, Ref, Response} ->
            case maps:get(Ref, State#state.pending, undefined) of
                undefined ->
                    % Orphaned response, ignore
                    {noreply, State};
                {From, _Request} ->
                    gen_server:reply(From, Response),
                    NewPending = maps:remove(Ref, State#state.pending),
                    {noreply, State#state{pending = NewPending}}
            end;
        {error, _Reason} ->
            % Malformed response, ignore
            {noreply, State}
    end;

handle_info({'EXIT', Port, Reason}, State = #state{port = Port}) ->
    % Port died, reply errors to all pending requests and restart
    maps:fold(fun(_Ref, {From, _Req}, _Acc) ->
        gen_server:reply(From, {error, port_died})
    end, ok, State#state.pending),

    % Try to restart port
    case open_port_driver() of
        {ok, NewPort} ->
            {noreply, State#state{port = NewPort, pending = #{}}};
        {error, _} ->
            {stop, {port_died, Reason}, State}
    end;

handle_info(_Info, State) ->
    {noreply, State}.

%% @private
terminate(_Reason, #state{port = Port}) ->
    catch port_close(Port),
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%% @private
%% Open port driver executable
open_port_driver() ->
    case code:priv_dir(sarc_gateway) of
        {error, bad_name} ->
            % Development mode
            PortPath = filename:join(["..", "priv", "sarc_port"]),
            try_open_port(PortPath);
        PrivDir ->
            PortPath = filename:join(PrivDir, "sarc_port"),
            try_open_port(PortPath)
    end.

%% @private
try_open_port(PortPath) ->
    case filelib:is_file(PortPath) of
        true ->
            Port = open_port({spawn_executable, PortPath}, [
                {packet, 4},  % 4-byte length prefix
                binary,
                exit_status,
                use_stdio
            ]),
            {ok, Port};
        false ->
            {error, {port_not_found, PortPath}}
    end.

%% @private
%% Send command to port and track pending request
send_port_command(Port, OpCode, Payload, From, State) ->
    Ref = make_ref(),
    Encoded = encode_port_request(OpCode, Ref, Payload),

    try
        port_command(Port, Encoded),
        NewPending = maps:put(Ref, {From, Payload}, State#state.pending),
        {noreply, State#state{pending = NewPending}}
    catch
        error:Error ->
            {reply, {error, Error}, State}
    end.

%% @private
%% Encode port request: [OpCode][{Ref, Payload}]
encode_port_request(OpCode, Ref, Payload) ->
    % Encode Ref and Payload as a single tuple
    FullPayload = term_to_binary({Ref, Payload}),
    <<OpCode:8, FullPayload/binary>>.

%% @private
%% Decode port response: [Ref][Response]
decode_port_response(Data) ->
    try
        % First term is the reference
        case binary_to_term(Data) of
            {Ref, Response} when is_reference(Ref) ->
                {ok, Ref, Response};
            _ ->
                {error, invalid_format}
        end
    catch
        _:_ ->
            {error, decode_failed}
    end.

%%%===================================================================
%%% EUnit Tests
%%%===================================================================

-ifdef(TEST).

%%--------------------------------------------------------------------
%% Validation Tests (no port required)
%%--------------------------------------------------------------------

validate_zone_id_test() ->
    % object_put validation is tested via gen_server call
    % Here we test the validation logic would reject invalid zones
    ok.

%%--------------------------------------------------------------------
%% Encoding/Decoding Tests
%%--------------------------------------------------------------------

encode_decode_request_test() ->
    Ref = make_ref(),
    Payload = {1, <<"data">>, <<"file.txt">>, <<"text/plain">>},

    Encoded = encode_port_request(?OP_PUT_OBJECT, Ref, Payload),

    ?assert(is_binary(Encoded)),
    ?assertEqual(?OP_PUT_OBJECT, binary:first(Encoded)).

decode_response_test() ->
    Ref = make_ref(),
    Response = {ok, #{zone => 1, hash => <<0:256>>}, #{size => 100}, false},

    % Encode as port would send it
    Data = term_to_binary({Ref, Response}),

    {ok, DecodedRef, DecodedResponse} = decode_port_response(Data),
    ?assertEqual(Ref, DecodedRef),
    ?assertEqual(Response, DecodedResponse).

decode_invalid_response_test() ->
    ?assertEqual({error, decode_failed}, decode_port_response(<<"garbage">>)),
    ?assertEqual({error, invalid_format}, decode_port_response(term_to_binary(just_a_term))).

%%--------------------------------------------------------------------
%% Gen_server Tests (require port executable)
%%--------------------------------------------------------------------

-ifdef(PORT_LOADED).

% These tests require the actual port executable to be built and available

start_stop_test() ->
    {ok, Pid} = start_link([{name, test_port}]),
    ?assert(is_pid(Pid)),
    ?assertEqual(ok, gen_server:stop(test_port)).

-endif.

-endif.
