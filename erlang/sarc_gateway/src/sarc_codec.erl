%%%-------------------------------------------------------------------
%%% @doc Binary encoding/decoding utilities for SARC
%%%
%%% Provides conversion functions between Erlang terms and SARC binary
%%% formats, including:
%%% - Hex string <-> binary conversion
%%% - ObjectKey encoding/decoding
%%% - Hash256 validation
%%% @end
%%%-------------------------------------------------------------------
-module(sarc_codec).

%% API exports
-export([
    hex_to_binary/1,
    binary_to_hex/1,
    encode_object_key/1,
    decode_object_key/1,
    validate_hash256/1,
    validate_zone_id/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type zone_id() :: 1..65535.
-type hash256() :: <<_:256>>.  % 32-byte binary
-type object_key() :: #{zone := zone_id(), hash := hash256()}.

-export_type([zone_id/0, hash256/0, object_key/0]).

%%%===================================================================
%%% API Functions
%%%===================================================================

%% @doc Convert hex string to binary
%%
%% Accepts both uppercase and lowercase hex strings.
%% Input must have even length (2 hex digits per byte).
%%
%% Example:
%%   hex_to_binary("deadbeef") -> {ok, <<222,173,190,239>>}
%%   hex_to_binary("DEADBEEF") -> {ok, <<222,173,190,239>>}
-spec hex_to_binary(string() | binary()) -> {ok, binary()} | {error, invalid_hex}.
hex_to_binary(Hex) when is_list(Hex) ->
    hex_to_binary(list_to_binary(Hex));
hex_to_binary(Hex) when is_binary(Hex) ->
    case byte_size(Hex) rem 2 of
        0 -> hex_to_binary_impl(Hex, <<>>);
        _ -> {error, invalid_hex}
    end.

%% @doc Convert binary to lowercase hex string
%%
%% Example:
%%   binary_to_hex(<<222,173,190,239>>) -> "deadbeef"
-spec binary_to_hex(binary()) -> string().
binary_to_hex(Bin) when is_binary(Bin) ->
    binary_to_hex_impl(Bin, []).

%% @doc Encode ObjectKey as binary
%%
%% Format: [2-byte zone (big-endian)][32-byte hash]
%% Total: 34 bytes
%%
%% Example:
%%   encode_object_key(#{zone => 1, hash => <<0:256>>}) -> {ok, <<0,1,0,0,...>>}
-spec encode_object_key(object_key()) -> {ok, binary()} | {error, term()}.
encode_object_key(#{zone := Zone, hash := Hash}) ->
    case validate_zone_id(Zone) of
        ok ->
            case validate_hash256(Hash) of
                ok -> {ok, <<Zone:16/big, Hash/binary>>};
                Error -> Error
            end;
        Error -> Error
    end;
encode_object_key(_) ->
    {error, invalid_object_key}.

%% @doc Decode ObjectKey from binary
%%
%% Expects 34-byte binary: [2-byte zone][32-byte hash]
%%
%% Example:
%%   decode_object_key(<<0,1,0,0,...>>) -> {ok, #{zone => 1, hash => <<0:256>>}}
-spec decode_object_key(binary()) -> {ok, object_key()} | {error, term()}.
decode_object_key(<<Zone:16/big, Hash:32/binary>>) ->
    case validate_zone_id(Zone) of
        ok -> {ok, #{zone => Zone, hash => Hash}};
        Error -> Error
    end;
decode_object_key(_) ->
    {error, invalid_binary_size}.

%% @doc Validate Hash256 (must be 32 bytes)
-spec validate_hash256(binary()) -> ok | {error, invalid_hash256}.
validate_hash256(<<_:256>>) -> ok;
validate_hash256(_) -> {error, invalid_hash256}.

%% @doc Validate ZoneId (must be 1..65535)
-spec validate_zone_id(integer()) -> ok | {error, invalid_zone_id}.
validate_zone_id(Zone) when is_integer(Zone), Zone >= 1, Zone =< 65535 -> ok;
validate_zone_id(_) -> {error, invalid_zone_id}.

%%%===================================================================
%%% Internal Functions
%%%===================================================================

%% @private
%% Convert hex binary to bytes
hex_to_binary_impl(<<>>, Acc) ->
    {ok, Acc};
hex_to_binary_impl(<<H1, H2, Rest/binary>>, Acc) ->
    case {hex_char_to_int(H1), hex_char_to_int(H2)} of
        {{ok, N1}, {ok, N2}} ->
            Byte = (N1 bsl 4) bor N2,
            hex_to_binary_impl(Rest, <<Acc/binary, Byte>>);
        _ ->
            {error, invalid_hex}
    end.

%% @private
%% Convert single hex character to integer
hex_char_to_int(C) when C >= $0, C =< $9 -> {ok, C - $0};
hex_char_to_int(C) when C >= $a, C =< $f -> {ok, C - $a + 10};
hex_char_to_int(C) when C >= $A, C =< $F -> {ok, C - $A + 10};
hex_char_to_int(_) -> error.

%% @private
%% Convert binary to hex string
binary_to_hex_impl(<<>>, Acc) ->
    lists:reverse(Acc);
binary_to_hex_impl(<<Byte, Rest/binary>>, Acc) ->
    H1 = int_to_hex_char(Byte bsr 4),
    H2 = int_to_hex_char(Byte band 16#0F),
    binary_to_hex_impl(Rest, [H2, H1 | Acc]).

%% @private
%% Convert integer (0-15) to hex character
int_to_hex_char(N) when N >= 0, N =< 9 -> N + $0;
int_to_hex_char(N) when N >= 10, N =< 15 -> N - 10 + $a.

%%%===================================================================
%%% EUnit Tests
%%%===================================================================

-ifdef(TEST).

%%--------------------------------------------------------------------
%% Hex Conversion Tests
%%--------------------------------------------------------------------

hex_to_binary_basic_test() ->
    ?assertEqual({ok, <<>>}, hex_to_binary("")),
    ?assertEqual({ok, <<0>>}, hex_to_binary("00")),
    ?assertEqual({ok, <<255>>}, hex_to_binary("ff")),
    ?assertEqual({ok, <<255>>}, hex_to_binary("FF")),
    ?assertEqual({ok, <<222, 173, 190, 239>>}, hex_to_binary("deadbeef")),
    ?assertEqual({ok, <<222, 173, 190, 239>>}, hex_to_binary("DEADBEEF")).

hex_to_binary_mixed_case_test() ->
    ?assertEqual({ok, <<222, 173, 190, 239>>}, hex_to_binary("DeAdBeEf")).

hex_to_binary_invalid_test() ->
    ?assertEqual({error, invalid_hex}, hex_to_binary("zz")),
    ?assertEqual({error, invalid_hex}, hex_to_binary("a")),  % Odd length
    ?assertEqual({error, invalid_hex}, hex_to_binary("abc")), % Odd length
    ?assertEqual({error, invalid_hex}, hex_to_binary("ag")).

binary_to_hex_basic_test() ->
    ?assertEqual("", binary_to_hex(<<>>)),
    ?assertEqual("00", binary_to_hex(<<0>>)),
    ?assertEqual("ff", binary_to_hex(<<255>>)),
    ?assertEqual("deadbeef", binary_to_hex(<<222, 173, 190, 239>>)).

hex_roundtrip_test() ->
    TestData = [
        <<>>,
        <<0>>,
        <<255>>,
        <<1, 2, 3, 4, 5>>,
        <<222, 173, 190, 239>>,
        crypto:strong_rand_bytes(32)  % Random 32-byte hash
    ],
    lists:foreach(fun(Bin) ->
        Hex = binary_to_hex(Bin),
        ?assertEqual({ok, Bin}, hex_to_binary(Hex))
    end, TestData).

%%--------------------------------------------------------------------
%% Hash256 Validation Tests
%%--------------------------------------------------------------------

validate_hash256_valid_test() ->
    Hash = <<0:256>>,  % 32 bytes of zeros
    ?assertEqual(ok, validate_hash256(Hash)).

validate_hash256_random_test() ->
    Hash = crypto:strong_rand_bytes(32),
    ?assertEqual(ok, validate_hash256(Hash)).

validate_hash256_invalid_size_test() ->
    ?assertEqual({error, invalid_hash256}, validate_hash256(<<>>)),
    ?assertEqual({error, invalid_hash256}, validate_hash256(<<0:8>>)),
    ?assertEqual({error, invalid_hash256}, validate_hash256(<<0:248>>)),  % 31 bytes
    ?assertEqual({error, invalid_hash256}, validate_hash256(<<0:264>>)).  % 33 bytes

%%--------------------------------------------------------------------
%% ZoneId Validation Tests
%%--------------------------------------------------------------------

validate_zone_id_valid_test() ->
    ?assertEqual(ok, validate_zone_id(1)),
    ?assertEqual(ok, validate_zone_id(100)),
    ?assertEqual(ok, validate_zone_id(65535)).

validate_zone_id_invalid_test() ->
    ?assertEqual({error, invalid_zone_id}, validate_zone_id(0)),
    ?assertEqual({error, invalid_zone_id}, validate_zone_id(-1)),
    ?assertEqual({error, invalid_zone_id}, validate_zone_id(65536)),
    ?assertEqual({error, invalid_zone_id}, validate_zone_id(100000)),
    ?assertEqual({error, invalid_zone_id}, validate_zone_id(not_an_integer)).

%%--------------------------------------------------------------------
%% ObjectKey Encoding Tests
%%--------------------------------------------------------------------

encode_object_key_valid_test() ->
    Hash = <<0:256>>,
    Key = #{zone => 1, hash => Hash},
    {ok, Encoded} = encode_object_key(Key),
    ?assertEqual(34, byte_size(Encoded)),
    ?assertEqual(<<0, 1, Hash/binary>>, Encoded).

encode_object_key_different_zones_test() ->
    Hash = crypto:strong_rand_bytes(32),

    {ok, Enc1} = encode_object_key(#{zone => 1, hash => Hash}),
    ?assertEqual(<<0, 1, Hash/binary>>, Enc1),

    {ok, Enc256} = encode_object_key(#{zone => 256, hash => Hash}),
    ?assertEqual(<<1, 0, Hash/binary>>, Enc256),

    {ok, Enc65535} = encode_object_key(#{zone => 65535, hash => Hash}),
    ?assertEqual(<<255, 255, Hash/binary>>, Enc65535).

encode_object_key_invalid_zone_test() ->
    Hash = <<0:256>>,
    ?assertEqual({error, invalid_zone_id}, encode_object_key(#{zone => 0, hash => Hash})),
    ?assertEqual({error, invalid_zone_id}, encode_object_key(#{zone => 65536, hash => Hash})).

encode_object_key_invalid_hash_test() ->
    ?assertEqual({error, invalid_hash256}, encode_object_key(#{zone => 1, hash => <<>>})),
    ?assertEqual({error, invalid_hash256}, encode_object_key(#{zone => 1, hash => <<0:248>>})).

encode_object_key_missing_fields_test() ->
    ?assertEqual({error, invalid_object_key}, encode_object_key(#{zone => 1})),
    ?assertEqual({error, invalid_object_key}, encode_object_key(#{hash => <<0:256>>})),
    ?assertEqual({error, invalid_object_key}, encode_object_key(#{})).

%%--------------------------------------------------------------------
%% ObjectKey Decoding Tests
%%--------------------------------------------------------------------

decode_object_key_valid_test() ->
    Hash = crypto:strong_rand_bytes(32),
    Encoded = <<0, 1, Hash/binary>>,
    {ok, Decoded} = decode_object_key(Encoded),
    ?assertEqual(#{zone => 1, hash => Hash}, Decoded).

decode_object_key_different_zones_test() ->
    Hash = crypto:strong_rand_bytes(32),

    {ok, Key1} = decode_object_key(<<0, 1, Hash/binary>>),
    ?assertEqual(1, maps:get(zone, Key1)),

    {ok, Key256} = decode_object_key(<<1, 0, Hash/binary>>),
    ?assertEqual(256, maps:get(zone, Key256)),

    {ok, Key65535} = decode_object_key(<<255, 255, Hash/binary>>),
    ?assertEqual(65535, maps:get(zone, Key65535)).

decode_object_key_invalid_size_test() ->
    ?assertEqual({error, invalid_binary_size}, decode_object_key(<<>>)),
    ?assertEqual({error, invalid_binary_size}, decode_object_key(<<0, 1>>)),
    ?assertEqual({error, invalid_binary_size}, decode_object_key(<<0:248>>)),  % 31 bytes
    ?assertEqual({error, invalid_binary_size}, decode_object_key(<<0:280>>)).  % 35 bytes

decode_object_key_invalid_zone_test() ->
    Hash = <<0:256>>,
    ?assertEqual({error, invalid_zone_id}, decode_object_key(<<0, 0, Hash/binary>>)).  % Zone 0

%%--------------------------------------------------------------------
%% ObjectKey Roundtrip Tests
%%--------------------------------------------------------------------

object_key_roundtrip_test() ->
    TestKeys = [
        #{zone => 1, hash => <<0:256>>},
        #{zone => 100, hash => crypto:strong_rand_bytes(32)},
        #{zone => 65535, hash => crypto:strong_rand_bytes(32)}
    ],
    lists:foreach(fun(Key) ->
        {ok, Encoded} = encode_object_key(Key),
        {ok, Decoded} = decode_object_key(Encoded),
        ?assertEqual(Key, Decoded)
    end, TestKeys).

-endif.
