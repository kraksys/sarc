#include <gtest/gtest.h>

// This test ensures that every public header compiles cleanly when included
// together (common for downstream users).

#include "sarc/bindings/erlang.hpp"
#include "sarc/bindings/http.hpp"
#include "sarc/cli/commands.hpp"
#include "sarc/cli/options.hpp"
#include "sarc/core/errors.hpp"
#include "sarc/core/models.hpp"
#include "sarc/core/types.hpp"
#include "sarc/core/zone.hpp"
#include "sarc/db/db.hpp"
#include "sarc/db/queries.hpp"
#include "sarc/db/schema.hpp"
#include "sarc/net/framing.hpp"
#include "sarc/net/protocol.hpp"
#include "sarc/security/crypto.hpp"
#include "sarc/security/kdf.hpp"
#include "sarc/security/keystore.hpp"
#include "sarc/security/policy.hpp"
#include "sarc/storage/buffer.hpp"
#include "sarc/storage/hashing.hpp"
#include "sarc/storage/layout.hpp"
#include "sarc/storage/object_store.hpp"

TEST(PublicHeaders, Compile) {
    SUCCEED();
}

