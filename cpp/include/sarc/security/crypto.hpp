#pragma once
#include <cstdint>
#include <type_traits>

#include "sarc/core/errors.hpp"
#include "sarc/core/models.hpp"
#include "sarc/core/types.hpp"

namespace sarc::security {
    using u8 = sarc::core::u8;
    using u16 = std::uint16_t;
    using u32 = std::uint32_t;

    struct Key256{
        u8 b[32]{};
    };

    struct Nonce12 {
        u8 b[12]{};
    };

    struct Tag16 {
        u8 b[16]{};
    };

    enum class AeadId : u8 {
        Aes256GcmSiv= 1,
    };

    struct BufferView {
        const u8* data{nullptr};
        u32 len{0};
    };

    struct BufferMut {
        u8* data{nullptr};
        u32 len{0};
    };

    struct AeadAad {
        sarc::core::ZoneId zone{ sarc::core::ZoneId::invalid() };
        sarc::core::ObjectKey object{};
        u64 size_bytes{0};
    };

    struct SealedHeader{
        u8 version{1};
        AeadId aead{AeadId::Aes256GcmSiv};
        u16 reserved{0};
        u32 aad_len{0};
        u32 ct_len{0};
        Nonce12 nonce{};
        Tag16 tag{};
    };

    // Nonce rules; Caller supplies unique nonce per item (key || zone || object)
    sarc::core::Status aead_seal(AeadId aead,
        const Key256& key,
        const Nonce12& nonce,
        BufferView aad,
        BufferView pt,
        BufferMut ct_out,
        Tag16* tag_out) noexcept;

    sarc::core::Status aead_open(AeadId aead,
        const Key256& key,
        const Nonce12& nonce,
        BufferView aad,
        BufferView ct,
        const Tag16& tag,
        BufferMut pt_out) noexcept;

    // HKDF interface
    sarc::core::Status hkdf_derive_object_key(const Key256& zone_root_key,
        const AeadAad& context,
        Key256* out_key) noexcept;

    static_assert(std::is_trivially_copyable_v<Key256>);
    static_assert(std::is_trivially_copyable_v<Nonce12>);
    static_assert(std::is_trivially_copyable_v<Tag16>);
    static_assert(std::is_trivially_copyable_v<SealedHeader>);

} // namespace sarc::security
