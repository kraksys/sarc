-module(sarc_auth).

-export([
    init/0,
    close/0,
    init_admin/2,
    pair_request/2,
    pair_approve/2,
    require_auth/6,
    parse_auth_headers/1,
    precheck/3,
    verify_signature/4,
    set_zone_shared/2,
    get_zone_shared/1,
    list_zone_members/1,
    list_zone_audit/2,
    log_action/6,
    sha256_hex/1,
    canonical_path/1
]).

-define(SIGN_WINDOW_MS, 120000).
-define(PAIR_TTL_MS, 300000).
-define(NONCE_TTL_MS, 300000).

-define(DEV_TABLE, sarc_auth_devices).
-define(META_TABLE, sarc_auth_meta).
-define(ZONE_TABLE, sarc_auth_zones).
-define(MEMBER_TABLE, sarc_auth_members).
-define(AUDIT_TABLE, sarc_auth_audit).
-define(PENDING_TABLE, sarc_auth_pending).
-define(NONCE_TABLE, sarc_auth_nonces).

init() ->
    ok = ensure_tables(),
    ok = ensure_ets(),
    ok.

close() ->
    _ = dets:close(?DEV_TABLE),
    _ = dets:close(?META_TABLE),
    _ = dets:close(?ZONE_TABLE),
    _ = dets:close(?MEMBER_TABLE),
    _ = dets:close(?AUDIT_TABLE),
    ok.

init_admin(PubKey, Label) when is_binary(PubKey), is_binary(Label) ->
    case get_admin_pubhash() of
        {ok, _} ->
            {error, already_initialized};
        not_found ->
            UserId = next_user_id(),
            PubHash = pubkey_hash(PubKey),
            Device = #{
                user_id => UserId,
                pubkey => PubKey,
                label => Label,
                is_admin => true,
                created_at => now_ms(),
                last_seen => now_ms()
            },
            dets:insert(?DEV_TABLE, {PubHash, Device}),
            dets:insert(?META_TABLE, {admin_pubhash, PubHash}),
            dets:insert(?META_TABLE, {next_user_id, UserId + 1}),
            {ok, #{user_id => UserId, fingerprint => hash_hex(PubHash)}}
    end.

pair_request(PubKey, Label) when is_binary(PubKey), is_binary(Label) ->
    case get_admin_pubhash() of
        not_found ->
            {error, no_admin};
        {ok, _} ->
            PubHash = pubkey_hash(PubKey),
            case dets:lookup(?DEV_TABLE, PubHash) of
                [_] -> {error, already_registered};
                [] ->
                    Code = gen_pair_code_unique(),
                    ExpiresAt = now_ms() + ?PAIR_TTL_MS,
                    ets:insert(?PENDING_TABLE, {Code, #{pubkey => PubKey, label => Label, expires_at => ExpiresAt}}),
                    {ok, #{code => Code, expires_in => ?PAIR_TTL_MS div 1000}}
            end
    end.

pair_approve(Code, AdminUser) when is_binary(Code), is_map(AdminUser) ->
    case ets:lookup(?PENDING_TABLE, Code) of
        [{_, #{pubkey := PubKey, label := Label, expires_at := ExpiresAt}}] ->
            case ExpiresAt >= now_ms() of
                true ->
                    ets:delete(?PENDING_TABLE, Code),
                    UserId = next_user_id(),
                    PubHash = pubkey_hash(PubKey),
                    Device = #{
                        user_id => UserId,
                        pubkey => PubKey,
                        label => Label,
                        is_admin => false,
                        created_at => now_ms(),
                        last_seen => now_ms()
                    },
                    dets:insert(?DEV_TABLE, {PubHash, Device}),
                    dets:insert(?META_TABLE, {next_user_id, UserId + 1}),
                    {ok, #{user_id => UserId, fingerprint => hash_hex(PubHash)}};
                false ->
                    ets:delete(?PENDING_TABLE, Code),
                    {error, expired}
            end;
        [] ->
            {error, invalid_code}
    end.

require_auth(Req, Method, Path, BodyHashHex, Zone, RequireAdmin) ->
    case parse_auth_headers(Req) of
        {ok, Auth} ->
            case precheck(Auth, Zone, RequireAdmin) of
                {ok, User} ->
                    case verify_signature(Auth, Method, Path, BodyHashHex) of
                        ok -> {ok, User};
                        {error, Reason} -> {error, Reason}
                    end;
                {error, Reason} -> {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

parse_auth_headers(Req) ->
    PubB64 = cowboy_req:header(<<"x-sarc-pub">>, Req),
    SigB64 = cowboy_req:header(<<"x-sarc-sig">>, Req),
    TsBin = cowboy_req:header(<<"x-sarc-ts">>, Req),
    NonceBin = cowboy_req:header(<<"x-sarc-nonce">>, Req),
    case {PubB64, SigB64, TsBin, NonceBin} of
        {undefined, _, _, _} -> {error, {401, <<"Missing auth pubkey">>}};
        {_, undefined, _, _} -> {error, {401, <<"Missing auth signature">>}};
        {_, _, undefined, _} -> {error, {401, <<"Missing auth timestamp">>}};
        {_, _, _, undefined} -> {error, {401, <<"Missing auth nonce">>}};
        _ ->
            case {b64_decode(PubB64), b64_decode(SigB64), parse_ts(TsBin), validate_nonce(NonceBin)} of
                {{ok, PubKey}, {ok, Sig}, {ok, Ts}, ok} ->
                    if byte_size(PubKey) =/= 32 -> {error, {401, <<"Invalid pubkey">>}};
                       byte_size(Sig) =/= 64 -> {error, {401, <<"Invalid signature">>}};
                       true ->
                           PubHash = pubkey_hash(PubKey),
                           {ok, #{
                               pubkey => PubKey,
                               pubhash => PubHash,
                               sig => Sig,
                               ts => Ts,
                               ts_bin => TsBin,
                               nonce => NonceBin
                           }}
                    end;
                {{error, _}, _, _, _} -> {error, {401, <<"Invalid pubkey encoding">>}};
                {_, {error, _}, _, _} -> {error, {401, <<"Invalid signature encoding">>}};
                {_, _, {error, _}, _} -> {error, {401, <<"Invalid timestamp">>}};
                {_, _, _, {error, Reason}} -> {error, {401, Reason}}
            end
    end.

precheck(Auth, Zone, RequireAdmin) ->
    #{pubhash := PubHash, ts := Ts} = Auth,
    case dets:lookup(?DEV_TABLE, PubHash) of
        [{_, Device}] ->
            case within_window(Ts) of
                true ->
                    case RequireAdmin andalso not maps:get(is_admin, Device, false) of
                        true -> {error, {403, <<"Admin required">>}};
                        false ->
                            case Zone of
                                undefined -> {ok, Device};
                                _ ->
                                    case zone_allowed(Device, Zone) of
                                        true -> {ok, Device};
                                        false -> {error, {403, <<"Zone not shared">>}}
                                    end
                            end
                    end;
                false ->
                    {error, {401, <<"Timestamp out of range">>}}
            end;
        [] ->
            {error, {401, <<"Unknown device">>}}
    end.

verify_signature(Auth, Method, Path, BodyHashHex) ->
    #{pubkey := PubKey, pubhash := PubHash, sig := Sig, ts_bin := TsBin, nonce := NonceBin, ts := Ts} = Auth,
    Canon = canonical_string(Method, Path, BodyHashHex, TsBin, NonceBin),
    case crypto:verify(eddsa, none, Canon, Sig, [PubKey, ed25519]) of
        true ->
            case nonce_unused(PubHash, NonceBin, Ts) of
                true ->
                    touch_device(PubHash),
                    ok;
                false ->
                    {error, {401, <<"Replay detected">>}}
            end;
        false ->
            {error, {401, <<"Invalid signature">>}}
    end.

set_zone_shared(Zone, Shared) when is_integer(Zone), is_boolean(Shared) ->
    dets:insert(?ZONE_TABLE, {Zone, Shared}),
    ok.

get_zone_shared(Zone) when is_integer(Zone) ->
    case dets:lookup(?ZONE_TABLE, Zone) of
        [{_, Shared}] -> Shared;
        [] -> false
    end.

list_zone_members(Zone) when is_integer(Zone) ->
    Entries = dets:match_object(?MEMBER_TABLE, {{Zone, '_'}, '_'}),
    UserIds = [UserId || {{_, UserId}, _} <- Entries],
    lists:usort([lookup_user(UserId) || UserId <- UserIds]).

list_zone_audit(Zone, Limit) when is_integer(Zone), is_integer(Limit) ->
    Entries = dets:lookup(?AUDIT_TABLE, Zone),
    Sorted = lists:sort(fun({_, A}, {_, B}) ->
        maps:get(ts, A, 0) >= maps:get(ts, B, 0)
    end, Entries),
    lists:sublist([Entry || {_, Entry} <- Sorted], Limit).

log_action(User, Zone, Action, Req, Status, Meta) ->
    UserId = maps:get(user_id, User, 0),
    Ip = safe_peer_ip(Req),
    Ua = safe_header(Req, <<"user-agent">>),
    Entry = #{
        ts => now_ms(),
        user_id => UserId,
        zone => Zone,
        action => Action,
        status => Status,
        meta => Meta,
        ip => Ip,
        ua => Ua
    },
    dets:insert(?AUDIT_TABLE, {Zone, Entry}),
    dets:insert(?MEMBER_TABLE, {{Zone, UserId}, true}),
    ok.

sha256_hex(Bin) when is_binary(Bin) ->
    Hash = crypto:hash(sha256, Bin),
    list_to_binary(sarc_codec:binary_to_hex(Hash)).

canonical_path(Req) ->
    Path = cowboy_req:path(Req),
    Qs = cowboy_req:qs(Req),
    case Qs of
        <<>> -> Path;
        _ -> <<Path/binary, "?", Qs/binary>>
    end.

%% ====================================================================
%% Internal helpers
%% ====================================================================

ensure_tables() ->
    Dir = auth_dir(),
    ok = filelib:ensure_dir(filename:join(Dir, "dummy")),
    ok = open_dets(?DEV_TABLE, filename:join(Dir, "auth_devices.dets"), set),
    ok = open_dets(?META_TABLE, filename:join(Dir, "auth_meta.dets"), set),
    ok = open_dets(?ZONE_TABLE, filename:join(Dir, "auth_zones.dets"), set),
    ok = open_dets(?MEMBER_TABLE, filename:join(Dir, "auth_members.dets"), set),
    ok = open_dets(?AUDIT_TABLE, filename:join(Dir, "auth_audit.dets"), bag),
    ok.

ensure_ets() ->
    _ = ensure_ets_table(?PENDING_TABLE),
    _ = ensure_ets_table(?NONCE_TABLE),
    ok.

ensure_ets_table(Name) ->
    case ets:info(Name) of
        undefined -> ets:new(Name, [set, public, named_table]);
        _ -> Name
    end.

open_dets(Name, File, Type) ->
    case dets:open_file(Name, [{file, File}, {type, Type}]) of
        {ok, _} -> ok;
        {error, {already_open, _}} -> ok;
        {error, Reason} -> exit({dets_open_failed, Name, Reason})
    end.

auth_dir() ->
    DataRoot = case os:getenv("SARC_DATA_ROOT") of
        false ->
            Home = case os:getenv("HOME") of false -> "/tmp"; H -> H end,
            filename:join(Home, "sarc");
        Dir -> Dir
    end,
    filename:join(DataRoot, "auth").

pubkey_hash(PubKey) ->
    crypto:hash(sha256, PubKey).

hash_hex(Bin) ->
    list_to_binary(sarc_codec:binary_to_hex(Bin)).

gen_pair_code() ->
    <<A:24>> = crypto:strong_rand_bytes(3),
    Num = A rem 1000000,
    list_to_binary(io_lib:format("~6..0B", [Num])).

gen_pair_code_unique() ->
    Code = gen_pair_code(),
    case ets:lookup(?PENDING_TABLE, Code) of
        [] -> Code;
        _ -> gen_pair_code_unique()
    end.

parse_ts(TsBin) ->
    try {ok, binary_to_integer(TsBin)} catch _:_ -> {error, invalid_ts} end.

validate_nonce(NonceBin) when is_binary(NonceBin) ->
    case byte_size(NonceBin) of
        32 ->
            case is_hex_binary(NonceBin) of
                true -> ok;
                false -> {error, <<"Invalid nonce">>}
            end;
        _ -> {error, <<"Invalid nonce">>}
    end.

is_hex_binary(Bin) ->
    lists:all(fun(C) ->
        (C >= $0 andalso C =< $9) orelse
        (C >= $a andalso C =< $f) orelse
        (C >= $A andalso C =< $F)
    end, binary_to_list(Bin)).

within_window(Ts) ->
    Delta = abs(now_ms() - Ts),
    Delta =< ?SIGN_WINDOW_MS.

nonce_unused(PubHash, Nonce, Ts) ->
    cleanup_nonces(),
    Key = {PubHash, Nonce},
    case ets:lookup(?NONCE_TABLE, Key) of
        [] ->
            ets:insert(?NONCE_TABLE, {Key, Ts}),
            true;
        _ ->
            false
    end.

cleanup_nonces() ->
    Now = now_ms(),
    Old = Now - ?NONCE_TTL_MS,
    case ets:info(?NONCE_TABLE, size) > 10000 of
        true ->
            ets:foldl(fun({Key, Ts}, Acc) ->
                if Ts < Old -> ets:delete(?NONCE_TABLE, Key); true -> ok end,
                Acc
            end, ok, ?NONCE_TABLE);
        false ->
            ok
    end.

zone_allowed(Device, Zone) ->
    case maps:get(is_admin, Device, false) of
        true -> true;
        false -> get_zone_shared(Zone)
    end.

touch_device(PubHash) ->
    case dets:lookup(?DEV_TABLE, PubHash) of
        [{_, Device}] ->
            Updated = Device#{last_seen => now_ms()},
            dets:insert(?DEV_TABLE, {PubHash, Updated}),
            ok;
        [] -> ok
    end.

lookup_user(UserId) ->
    dets:foldl(fun({PubHash, Device}, Acc) ->
        case maps:get(user_id, Device, -1) of
            UserId ->
                #{
                    user_id => UserId,
                    label => maps:get(label, Device, <<"">>),
                    is_admin => maps:get(is_admin, Device, false),
                    fingerprint => hash_hex(PubHash)
                };
            _ -> Acc
        end
    end, #{user_id => UserId}, ?DEV_TABLE).

get_admin_pubhash() ->
    case dets:lookup(?META_TABLE, admin_pubhash) of
        [{_, PubHash}] -> {ok, PubHash};
        [] -> not_found
    end.

next_user_id() ->
    case dets:lookup(?META_TABLE, next_user_id) of
        [{_, Id}] -> Id;
        [] -> 1
    end.

safe_peer_ip(Req) ->
    case catch cowboy_req:peer(Req) of
        {Ip, _Port} ->
            list_to_binary(inet:ntoa(Ip));
        _ ->
            <<"">>
    end.

safe_header(Req, Name) ->
    case catch cowboy_req:header(Name, Req, <<"">>) of
        {'EXIT', _} -> <<"">>;
        Val -> Val
    end.

canonical_string(Method, Path, BodyHashHex, TsBin, NonceBin) ->
    <<Method/binary, "\n", Path/binary, "\n", BodyHashHex/binary, "\n", TsBin/binary, "\n", NonceBin/binary>>.

b64_decode(Bin) when is_binary(Bin) ->
    try {ok, base64:decode(Bin)} catch _:_ -> {error, invalid_b64} end.

now_ms() ->
    erlang:system_time(millisecond).
