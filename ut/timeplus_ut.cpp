#include <timeplus/block.h>
#include <timeplus/client.h>
#include <timeplus/columns/numeric.h>
#include <timeplus/exceptions.h>
#include <timeplus/timeplus.h>

#include <gtest/gtest.h>

#include <chrono>
#include <condition_variable>
#include <mutex>

using namespace timeplus;

TEST(TimeplusInsert, DISABLED_InsertSync) {
    const auto* host = "localhost";
    const uint16_t port = 8463;

    ClientOptions client_options;
    client_options.host = host;
    client_options.port = port;
    Client client{client_options};
    /// Create test table.
    client.Execute("DROP STREAM IF EXISTS test_timeplus_insert_sync");
    client.Execute("CREATE STREAM test_timeplus_insert_sync(id uint64)");

    TimeplusConfig config;
    config.client_options.host = host;
    config.client_options.port = port;
    config.max_connections = 1;
    Timeplus tp{std::move(config)};

    auto b = std::make_shared<Block>();
    auto id = std::make_shared<ColumnUInt64>();
    id->Append(5);
    id->Append(7);
    id->Append(4);
    id->Append(8);
    b->AppendColumn("id", id);

    /// Insert for multiple times.
    ASSERT_TRUE(tp.Insert("test_timeplus_insert_sync", b).ok());
    ASSERT_TRUE(tp.Insert("test_timeplus_insert_sync", b).ok());
    ASSERT_TRUE(tp.Insert("test_timeplus_insert_sync", b).ok());
    ASSERT_TRUE(tp.Insert("test_timeplus_insert_sync", b).ok());
    ASSERT_TRUE(tp.Insert("test_timeplus_insert_sync", b).ok());

    const uint64_t VALUE[] = {5, 7, 4, 8};
    size_t row = 0;
    while (row != 20U) {
        client.Select("SELECT id FROM table(test_timeplus_insert_sync)", [VALUE, &row](const Block& block) {
            if (block.GetRowCount() == 0) {
                return;
            }
            EXPECT_EQ(block.GetColumnCount(), 1U);
            for (size_t c = 0; c < block.GetRowCount(); ++c, ++row) {
                auto col = block[0]->As<ColumnUInt64>();
                EXPECT_EQ(VALUE[row % 4], col->At(c));
            }
        });
    }
}

TEST(TimeplusInsert, DISABLED_InsertAsync) {
    const auto* host = "localhost";
    const uint16_t port = 8463;

    ClientOptions client_options;
    client_options.host = host;
    client_options.port = port;
    Client client{client_options};
    /// Create test table.
    client.Execute("DROP STREAM IF EXISTS test_timeplus_insert_async");
    client.Execute("CREATE STREAM test_timeplus_insert_async(id uint64)");

    TimeplusConfig config;
    config.client_options.endpoints.push_back({host, port});
    config.max_connections = 5;
    config.task_executors = 5;
    Timeplus tp{std::move(config)};

    auto b = std::make_shared<Block>();
    auto id = std::make_shared<ColumnUInt64>();
    id->Append(5);
    id->Append(7);
    id->Append(4);
    id->Append(8);
    b->AppendColumn("id", id);

    std::mutex insert_done_mutex;
    std::condition_variable insert_done_cv;
    uint64_t insert_done = 0;
    bool insert_sucess = true;
    auto callback = [&](const BaseResult& result) {
        const auto& insert_result = dynamic_cast<const InsertResult&>(result);
        {
            std::lock_guard lk{insert_done_mutex};
            ++insert_done;
            if (!insert_result.ok()) {
                insert_sucess = false;
            }
        }
        insert_done_cv.notify_one();
    };

    /// Insert for multiple times.
    constexpr uint64_t num_inserts = 100;
    for (uint64_t i = 0; i < num_inserts; ++i) {
        tp.InsertAsync("test_timeplus_insert_async", b, callback);
    }

    std::unique_lock lk{insert_done_mutex};
    insert_done_cv.wait(lk, [&insert_done] { return insert_done == num_inserts; });
    ASSERT_TRUE(insert_sucess);

    const uint64_t VALUE[] = {5, 7, 4, 8};
    size_t row = 0;
    while (row != num_inserts * 4) {
        client.Select("SELECT id FROM table(test_timeplus_insert_async)", [VALUE, &row](const Block& block) {
            if (block.GetRowCount() == 0) {
                return;
            }
            EXPECT_EQ(block.GetColumnCount(), 1U);
            for (size_t c = 0; c < block.GetRowCount(); ++c, ++row) {
                auto col = block[0]->As<ColumnUInt64>();
                EXPECT_EQ(VALUE[row % 4], col->At(c));
            }
        });
    }
}

TEST(TimeplusInsert, DISABLED_IdempotentInsert) {
    const auto* host = "localhost";
    const uint16_t port = 8463;

    ClientOptions client_options;
    client_options.host = host;
    client_options.port = port;
    Client client{client_options};
    /// Create test table.
    client.Execute("DROP STREAM IF EXISTS test_timeplus_insert_idempotent");
    client.Execute("CREATE STREAM test_timeplus_insert_idempotent(id uint64)");

    TimeplusConfig config;
    config.client_options.host = host;
    config.client_options.port = port;
    config.max_connections = 10;
    config.task_executors = 10;
    Timeplus tp{std::move(config)};

    auto b = std::make_shared<Block>();
    auto id = std::make_shared<ColumnUInt64>();
    id->Append(5);
    id->Append(7);
    id->Append(4);
    id->Append(8);
    b->AppendColumn("id", id);

    std::mutex insert_done_mutex;
    std::condition_variable insert_done_cv;
    uint64_t insert_done = 0;
    bool insert_success = true;
    auto callback = [&](const BaseResult& result) {
        const auto& insert_result = dynamic_cast<const InsertResult&>(result);
        {
            std::lock_guard lk{insert_done_mutex};
            ++insert_done;
            if (!insert_result.ok()) {
                insert_success = false;
            }
        }
        insert_done_cv.notify_one();
    };

    /// Insert for multiple times.
    constexpr uint64_t num_inserts = 100;
    constexpr uint64_t num_idempotent_id = 7;
    for (uint64_t i = 0; i < num_inserts; ++i) {
        tp.InsertAsync("test_timeplus_insert_idempotent", b,
                       /*idempotent_id=*/"idempotent-id-" + std::to_string(i % num_idempotent_id), callback);
    }

    std::unique_lock lk{insert_done_mutex};
    insert_done_cv.wait(lk, [&insert_done] { return insert_done == num_inserts; });
    ASSERT_TRUE(insert_success);

    const uint64_t VALUE[] = {5, 7, 4, 8};
    size_t row = 0;
    while (row < num_idempotent_id * 4) {
        client.Select("SELECT id FROM table(test_timeplus_insert_idempotent)", [VALUE, &row](const Block& block) {
            if (block.GetRowCount() == 0) {
                return;
            }
            EXPECT_EQ(block.GetColumnCount(), 1U);
            for (size_t c = 0; c < block.GetRowCount(); ++c, ++row) {
                auto col = block[0]->As<ColumnUInt64>();
                EXPECT_EQ(VALUE[row % 4], col->At(c));
            }
        });
    }
    EXPECT_EQ(row, num_idempotent_id * 4);
}

TEST(TimeplusInsert, InvalidColumnName) {
    const auto* host = "localhost";
    const uint16_t port = 8463;

    ClientOptions client_options;
    client_options.host = host;
    client_options.port = port;
    Client client{client_options};
    /// Create test table.
    client.Execute("DROP STREAM IF EXISTS test_timeplus_insert_invalid_col_name");
    client.Execute("CREATE STREAM test_timeplus_insert_invalid_col_name(id uint64)");

    TimeplusConfig config;
    config.client_options.endpoints.push_back({host, port});
    config.max_connections = 1;
    Timeplus tp{std::move(config)};

    auto b = std::make_shared<Block>();
    auto id = std::make_shared<ColumnUInt64>();
    id->Append(5);
    id->Append(7);
    id->Append(4);
    id->Append(8);
    b->AppendColumn("id1", id);

    /// Synchronous insert.
    auto result = tp.Insert("test_timeplus_insert_invalid_col_name", b);
    ASSERT_EQ(result.err_code, ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

    /// Asynchronous insert.
    std::mutex insert_done_mutex;
    std::condition_variable insert_done_cv;
    bool insert_done = false;
    tp.InsertAsync("test_timeplus_insert_invalid_col_name", b, [&](const auto& result) {
        EXPECT_EQ(result.err_code, ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);
        {
            std::lock_guard lk{insert_done_mutex};
            insert_done = true;
        }
        insert_done_cv.notify_one();
    });
    std::unique_lock lk{insert_done_mutex};
    insert_done_cv.wait(lk, [&] { return insert_done; });
}

TEST(TimeplusInsert, InvalidHostPort) {
    const auto* host = "100::1";
    const uint16_t port = 5748;

    TimeplusConfig config;
    config.client_options.host = host;
    config.client_options.port = port;
    config.client_options.connection_recv_timeout = std::chrono::milliseconds(500);
    config.client_options.connection_send_timeout = std::chrono::milliseconds(500);
    config.max_connections = 3;
    config.client_acquire_timeout_ms = 1000;
    config.wait_time_before_retry_ms = 0;
    Timeplus tp{std::move(config)};

    auto b = std::make_shared<Block>();
    auto id = std::make_shared<ColumnUInt64>();
    id->Append(5);
    id->Append(7);
    id->Append(4);
    id->Append(8);
    b->AppendColumn("id", id);

    /// Synchronous insert.
    auto insert_result = tp.Insert("test_timeplus_insert_invalid_host_port", b);
    ASSERT_EQ(insert_result.err_code, ErrorCodes::NETWORK_ERROR);

    /// Asynchronous insert.
    std::mutex insert_done_mutex;
    std::condition_variable insert_done_cv;
    bool insert_done = false;
    tp.InsertAsync("test_timeplus_insert_invalid_host_port", b, [&](const auto& result) {
        EXPECT_EQ(result.err_code, ErrorCodes::NETWORK_ERROR);
        {
            std::lock_guard lk{insert_done_mutex};
            insert_done = true;
        }
        insert_done_cv.notify_one();
    });
    std::unique_lock lk{insert_done_mutex};
    insert_done_cv.wait(lk, [&] { return insert_done; });
}
