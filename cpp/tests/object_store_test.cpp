#include <gtest/gtest.h>
#include "sarc/storage/object_store.hpp"
#include "sarc/core/zone.hpp"
#include <vector>
#include <thread>
#include <cstring>
#include <sys/stat.h>

using namespace sarc::storage;
using namespace sarc::core;

// Test fixture for object store tests
class ObjectStoreTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Use temporary directory for testing
        config_.data_root = "/tmp/sarc_test_objects";
        config_.db_path = "/tmp/sarc_test.db";
        config_.max_object_bytes = 0;  // Unlimited
        config_.compression = CompressionPolicy::None;
        config_.verify_on_read = false;

        // Clean up any existing test data
        system("rm -rf /tmp/sarc_test_objects");
        system("rm -f /tmp/sarc_test.db");

        // Create test directory
        mkdir("/tmp/sarc_test_objects", 0755);

        // Initialize object store
        Status s = object_store_init(config_);
        ASSERT_TRUE(is_ok(s)) << "Failed to initialize object store";
    }

    void TearDown() override {
        object_store_shutdown();

        // Clean up test data
        system("rm -rf /tmp/sarc_test_objects");
        system("rm -f /tmp/sarc_test.db");
    }

    ObjectStoreConfig config_;
};

// ============================================================================
// Basic Operations Tests
// ============================================================================

TEST_F(ObjectStoreTest, PutAndGetSmallObject) {
    // Create test data (4KB)
    std::vector<u8> data(4096);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = static_cast<u8>(i & 0xFF);
    }

    // Put object
    ObjectPutParams put_params;
    put_params.zone = ZoneId{1};
    put_params.data = data.data();
    put_params.size_bytes = data.size();
    put_params.filename = "test_file.bin";
    put_params.mime_type = "application/octet-stream";

    ObjectPutResult put_result;
    Status s = object_put(put_params, &put_result);
    ASSERT_TRUE(is_ok(s));
    EXPECT_FALSE(put_result.deduplicated);
    EXPECT_EQ(put_result.meta.size_bytes, 4096);
    EXPECT_EQ(put_result.meta.refcount, 1);

    // Get object back
    std::vector<u8> buffer(4096);
    ObjectGetResult get_result;
    s = object_get(put_result.key, buffer.data(), buffer.size(), &get_result);
    ASSERT_TRUE(is_ok(s));
    EXPECT_EQ(get_result.bytes_read, 4096);
    EXPECT_EQ(get_result.meta.size_bytes, 4096);

    // Verify data
    EXPECT_EQ(buffer, data);
}

TEST_F(ObjectStoreTest, PutAndGetLargeObject) {
    // Create test data (1MB)
    std::vector<u8> data(1024 * 1024);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = static_cast<u8>((i * 7) & 0xFF);
    }

    // Put object
    ObjectPutParams put_params;
    put_params.zone = ZoneId{1};
    put_params.data = data.data();
    put_params.size_bytes = data.size();
    put_params.filename = "large_file.bin";
    put_params.mime_type = nullptr;

    ObjectPutResult put_result;
    Status s = object_put(put_params, &put_result);
    ASSERT_TRUE(is_ok(s));

    // Get object back
    std::vector<u8> buffer(1024 * 1024);
    ObjectGetResult get_result;
    s = object_get(put_result.key, buffer.data(), buffer.size(), &get_result);
    ASSERT_TRUE(is_ok(s));
    EXPECT_EQ(get_result.bytes_read, data.size());

    // Verify data
    EXPECT_EQ(buffer, data);
}

TEST_F(ObjectStoreTest, PutAndGetEmptyObject) {
    // Empty object
    ObjectPutParams put_params;
    put_params.zone = ZoneId{1};
    put_params.data = nullptr;
    put_params.size_bytes = 0;
    put_params.filename = "empty.bin";
    put_params.mime_type = nullptr;

    ObjectPutResult put_result;
    Status s = object_put(put_params, &put_result);
    ASSERT_TRUE(is_ok(s));

    // Get it back
    u8 dummy_buffer;
    ObjectGetResult get_result;
    s = object_get(put_result.key, &dummy_buffer, 1, &get_result);
    ASSERT_TRUE(is_ok(s));
    EXPECT_EQ(get_result.bytes_read, 0);
}

// ============================================================================
// Deduplication Tests
// ============================================================================

TEST_F(ObjectStoreTest, DeduplicationSameContent) {
    // Create test data
    std::vector<u8> data(1024, 0xAB);

    // Put same content twice
    ObjectPutParams put_params;
    put_params.zone = ZoneId{1};
    put_params.data = data.data();
    put_params.size_bytes = data.size();
    put_params.filename = "file1.bin";
    put_params.mime_type = nullptr;

    ObjectPutResult result1;
    Status s = object_put(put_params, &result1);
    ASSERT_TRUE(is_ok(s));
    EXPECT_FALSE(result1.deduplicated);
    EXPECT_EQ(result1.meta.refcount, 1);

    // Put again with same content
    put_params.filename = "file2.bin";  // Different filename, same content
    ObjectPutResult result2;
    s = object_put(put_params, &result2);
    ASSERT_TRUE(is_ok(s));
    EXPECT_TRUE(result2.deduplicated);  // Should be deduplicated
    EXPECT_EQ(result2.meta.refcount, 2);  // Refcount incremented

    // Keys should be identical (same content hash)
    EXPECT_EQ(result1.key.zone.v, result2.key.zone.v);
    for (size_t i = 0; i < 32; ++i) {
        EXPECT_EQ(result1.key.content.b[i], result2.key.content.b[i]);
    }
}

TEST_F(ObjectStoreTest, DeduplicationDifferentContent) {
    // Create different test data
    std::vector<u8> data1(1024, 0xAA);
    std::vector<u8> data2(1024, 0xBB);

    ObjectPutParams put_params;
    put_params.zone = ZoneId{1};
    put_params.filename = nullptr;
    put_params.mime_type = nullptr;

    // Put first object
    put_params.data = data1.data();
    put_params.size_bytes = data1.size();
    ObjectPutResult result1;
    Status s = object_put(put_params, &result1);
    ASSERT_TRUE(is_ok(s));
    EXPECT_FALSE(result1.deduplicated);

    // Put second object with different content
    put_params.data = data2.data();
    put_params.size_bytes = data2.size();
    ObjectPutResult result2;
    s = object_put(put_params, &result2);
    ASSERT_TRUE(is_ok(s));
    EXPECT_FALSE(result2.deduplicated);  // Different content, not deduplicated

    // Keys should be different
    bool keys_different = false;
    for (size_t i = 0; i < 32; ++i) {
        if (result1.key.content.b[i] != result2.key.content.b[i]) {
            keys_different = true;
            break;
        }
    }
    EXPECT_TRUE(keys_different);
}

// ============================================================================
// Exists Tests
// ============================================================================

TEST_F(ObjectStoreTest, ExistsTrue) {
    std::vector<u8> data(100, 0xCD);

    ObjectPutParams put_params;
    put_params.zone = ZoneId{1};
    put_params.data = data.data();
    put_params.size_bytes = data.size();
    put_params.filename = nullptr;
    put_params.mime_type = nullptr;

    ObjectPutResult put_result;
    Status s = object_put(put_params, &put_result);
    ASSERT_TRUE(is_ok(s));

    // Check exists
    bool exists = false;
    s = object_exists(put_result.key, &exists);
    ASSERT_TRUE(is_ok(s));
    EXPECT_TRUE(exists);
}

TEST_F(ObjectStoreTest, ExistsFalse) {
    // Create a key that doesn't exist
    ObjectKey key;
    key.zone = ZoneId{1};
    for (auto& b : key.content.b) {
        b = 0xFF;
    }

    bool exists = true;
    Status s = object_exists(key, &exists);
    ASSERT_TRUE(is_ok(s));
    EXPECT_FALSE(exists);
}

// ============================================================================
// Delete and Refcount Tests
// ============================================================================

TEST_F(ObjectStoreTest, DeleteSingleReference) {
    std::vector<u8> data(512, 0xEF);

    ObjectPutParams put_params;
    put_params.zone = ZoneId{1};
    put_params.data = data.data();
    put_params.size_bytes = data.size();
    put_params.filename = nullptr;
    put_params.mime_type = nullptr;

    ObjectPutResult put_result;
    Status s = object_put(put_params, &put_result);
    ASSERT_TRUE(is_ok(s));

    // Delete
    s = object_delete(put_result.key);
    ASSERT_TRUE(is_ok(s));

    // Object should still exist (refcount=0, needs GC)
    bool exists = false;
    s = object_exists(put_result.key, &exists);
    ASSERT_TRUE(is_ok(s));
    EXPECT_TRUE(exists);
}

TEST_F(ObjectStoreTest, DeleteMultipleReferences) {
    std::vector<u8> data(256, 0x12);

    ObjectPutParams put_params;
    put_params.zone = ZoneId{1};
    put_params.data = data.data();
    put_params.size_bytes = data.size();
    put_params.filename = nullptr;
    put_params.mime_type = nullptr;

    // Put twice to get refcount=2
    ObjectPutResult result1;
    Status s = object_put(put_params, &result1);
    ASSERT_TRUE(is_ok(s));

    ObjectPutResult result2;
    s = object_put(put_params, &result2);
    ASSERT_TRUE(is_ok(s));
    EXPECT_EQ(result2.meta.refcount, 2);

    // Delete once
    s = object_delete(result1.key);
    ASSERT_TRUE(is_ok(s));

    // Object should still exist with refcount=1
    bool exists = false;
    s = object_exists(result1.key, &exists);
    ASSERT_TRUE(is_ok(s));
    EXPECT_TRUE(exists);

    // Should still be able to get it
    std::vector<u8> buffer(256);
    ObjectGetResult get_result;
    s = object_get(result1.key, buffer.data(), buffer.size(), &get_result);
    ASSERT_TRUE(is_ok(s));
    EXPECT_EQ(buffer, data);
}

// ============================================================================
// Error Cases
// ============================================================================

TEST_F(ObjectStoreTest, GetNotFound) {
    ObjectKey key;
    key.zone = ZoneId{1};
    for (auto& b : key.content.b) {
        b = 0xAA;
    }

    std::vector<u8> buffer(100);
    ObjectGetResult result;
    Status s = object_get(key, buffer.data(), buffer.size(), &result);
    EXPECT_FALSE(is_ok(s));
    EXPECT_EQ(s.code, StatusCode::NotFound);
}

TEST_F(ObjectStoreTest, GetBufferTooSmall) {
    std::vector<u8> data(1024, 0x77);

    ObjectPutParams put_params;
    put_params.zone = ZoneId{1};
    put_params.data = data.data();
    put_params.size_bytes = data.size();
    put_params.filename = nullptr;
    put_params.mime_type = nullptr;

    ObjectPutResult put_result;
    Status s = object_put(put_params, &put_result);
    ASSERT_TRUE(is_ok(s));

    // Try to get with too small buffer
    std::vector<u8> small_buffer(512);
    ObjectGetResult get_result;
    s = object_get(put_result.key, small_buffer.data(), small_buffer.size(), &get_result);
    EXPECT_FALSE(is_ok(s));
    EXPECT_EQ(s.code, StatusCode::Invalid);
    EXPECT_EQ(s.aux, 1024);  // Should return required size
}

TEST_F(ObjectStoreTest, PutInvalidZone) {
    std::vector<u8> data(100, 0x88);

    ObjectPutParams put_params;
    put_params.zone = ZoneId{0};  // Universal zone (invalid for storage)
    put_params.data = data.data();
    put_params.size_bytes = data.size();
    put_params.filename = nullptr;
    put_params.mime_type = nullptr;

    ObjectPutResult result;
    Status s = object_put(put_params, &result);
    EXPECT_FALSE(is_ok(s));
    EXPECT_EQ(s.code, StatusCode::Invalid);
}

TEST_F(ObjectStoreTest, DeleteNotFound) {
    ObjectKey key;
    key.zone = ZoneId{1};
    for (auto& b : key.content.b) {
        b = 0xBB;
    }

    Status s = object_delete(key);
    EXPECT_FALSE(is_ok(s));
    EXPECT_EQ(s.code, StatusCode::NotFound);
}

// ============================================================================
// Parallel Operations Tests
// ============================================================================

TEST_F(ObjectStoreTest, ParallelPut) {
    const int num_threads = 8;
    const int objects_per_thread = 100;
    std::vector<std::thread> threads;
    std::vector<bool> success(num_threads, false);

    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([t, &success, objects_per_thread]() {
            bool thread_success = true;

            for (int i = 0; i < objects_per_thread; ++i) {
                // Create unique data for each thread/object
                std::vector<u8> data(128);
                u32 seed = (t * 1000 + i);
                for (size_t j = 0; j < data.size(); ++j) {
                    data[j] = static_cast<u8>((seed + j) & 0xFF);
                }

                ObjectPutParams put_params;
                put_params.zone = ZoneId{static_cast<u32>(t + 1)};
                put_params.data = data.data();
                put_params.size_bytes = data.size();
                put_params.filename = nullptr;
                put_params.mime_type = nullptr;

                ObjectPutResult result;
                Status s = object_put(put_params, &result);
                if (!is_ok(s)) {
                    thread_success = false;
                    break;
                }
            }

            success[t] = thread_success;
        });
    }

    // Wait for all threads
    for (auto& thread : threads) {
        thread.join();
    }

    // Verify all threads succeeded
    for (int t = 0; t < num_threads; ++t) {
        EXPECT_TRUE(success[t]) << "Thread " << t << " failed";
    }
}

TEST_F(ObjectStoreTest, ParallelGetSameObject) {
    // Put one object
    std::vector<u8> data(1024);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = static_cast<u8>(i & 0xFF);
    }

    ObjectPutParams put_params;
    put_params.zone = ZoneId{1};
    put_params.data = data.data();
    put_params.size_bytes = data.size();
    put_params.filename = nullptr;
    put_params.mime_type = nullptr;

    ObjectPutResult put_result;
    Status s = object_put(put_params, &put_result);
    ASSERT_TRUE(is_ok(s));

    // Read from multiple threads
    const int num_threads = 16;
    std::vector<std::thread> threads;
    std::vector<bool> success(num_threads, false);

    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([t, &success, &put_result, &data]() {
            std::vector<u8> buffer(1024);
            ObjectGetResult get_result;
            Status s = object_get(put_result.key, buffer.data(), buffer.size(), &get_result);

            success[t] = is_ok(s) && (buffer == data);
        });
    }

    // Wait for all threads
    for (auto& thread : threads) {
        thread.join();
    }

    // Verify all threads succeeded
    for (int t = 0; t < num_threads; ++t) {
        EXPECT_TRUE(success[t]) << "Thread " << t << " failed";
    }
}

TEST_F(ObjectStoreTest, ParallelPutSameContent) {
    // Multiple threads putting identical content (should deduplicate)
    const int num_threads = 8;
    std::vector<u8> data(256, 0x99);
    std::vector<std::thread> threads;
    std::vector<ObjectPutResult> results(num_threads);
    std::vector<bool> success(num_threads, false);

    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([t, &data, &results, &success]() {
            ObjectPutParams put_params;
            put_params.zone = ZoneId{1};
            put_params.data = data.data();
            put_params.size_bytes = data.size();
            put_params.filename = nullptr;
            put_params.mime_type = nullptr;

            Status s = object_put(put_params, &results[t]);
            success[t] = is_ok(s);
        });
    }

    // Wait for all threads
    for (auto& thread : threads) {
        thread.join();
    }

    // Verify all threads succeeded
    for (int t = 0; t < num_threads; ++t) {
        EXPECT_TRUE(success[t]) << "Thread " << t << " failed";
    }

    // All should have same content hash (deduplication)
    for (int t = 1; t < num_threads; ++t) {
        for (size_t i = 0; i < 32; ++i) {
            EXPECT_EQ(results[0].key.content.b[i], results[t].key.content.b[i]);
        }
    }
}

// ============================================================================
// Zone Isolation Tests
// ============================================================================

TEST_F(ObjectStoreTest, SameContentDifferentZones) {
    std::vector<u8> data(512, 0x55);

    // Put same content in zone 1
    ObjectPutParams put_params;
    put_params.zone = ZoneId{1};
    put_params.data = data.data();
    put_params.size_bytes = data.size();
    put_params.filename = nullptr;
    put_params.mime_type = nullptr;

    ObjectPutResult result1;
    Status s = object_put(put_params, &result1);
    ASSERT_TRUE(is_ok(s));

    // Put same content in zone 2
    put_params.zone = ZoneId{2};
    ObjectPutResult result2;
    s = object_put(put_params, &result2);
    ASSERT_TRUE(is_ok(s));

    // Should NOT be deduplicated (different zones)
    EXPECT_FALSE(result2.deduplicated);

    // Content hash should be same
    bool content_same = true;
    for (size_t i = 0; i < 32; ++i) {
        if (result1.key.content.b[i] != result2.key.content.b[i]) {
            content_same = false;
            break;
        }
    }
    EXPECT_TRUE(content_same);

    // But zone should be different
    EXPECT_NE(result1.key.zone.v, result2.key.zone.v);
}

// ============================================================================
// Verification Tests
// ============================================================================

TEST_F(ObjectStoreTest, VerificationDisabled) {
    // Verification is disabled by default in SetUp
    EXPECT_FALSE(config_.verify_on_read);

    std::vector<u8> data(256, 0xAA);

    ObjectPutParams put_params;
    put_params.zone = ZoneId{1};
    put_params.data = data.data();
    put_params.size_bytes = data.size();
    put_params.filename = nullptr;
    put_params.mime_type = nullptr;

    ObjectPutResult put_result;
    Status s = object_put(put_params, &put_result);
    ASSERT_TRUE(is_ok(s));

    // Get should succeed without verification
    std::vector<u8> buffer(256);
    ObjectGetResult get_result;
    s = object_get(put_result.key, buffer.data(), buffer.size(), &get_result);
    ASSERT_TRUE(is_ok(s));
    EXPECT_EQ(buffer, data);
}

// ============================================================================
// Filesystem Path Tests
// ============================================================================

TEST_F(ObjectStoreTest, FilesystemPathStructure) {
    std::vector<u8> data(100, 0x42);

    ObjectPutParams put_params;
    put_params.zone = ZoneId{5};
    put_params.data = data.data();
    put_params.size_bytes = data.size();
    put_params.filename = nullptr;
    put_params.mime_type = nullptr;

    ObjectPutResult put_result;
    Status s = object_put(put_params, &put_result);
    ASSERT_TRUE(is_ok(s));

    // Verify filesystem structure exists
    // Path should be: /tmp/sarc_test_objects/5/{hash[0:2]}/{hash[2:4]}/{hash}.dat

    // Just verify the base directory for zone 5 exists
    struct stat st;
    int result = stat("/tmp/sarc_test_objects/5", &st);
    EXPECT_EQ(result, 0) << "Zone directory should exist";
    EXPECT_TRUE(S_ISDIR(st.st_mode)) << "Should be a directory";
}
