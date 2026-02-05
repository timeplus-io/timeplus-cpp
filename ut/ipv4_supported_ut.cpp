#include <timeplus/client.h>
#include <timeplus/columns/ip4.h>

#include "ut/utils.h"
#include "ut/value_generators.h"

#include <gtest/gtest.h>

using namespace timeplus;

// Test inserting and selecting IPv4 addresses
TEST(IPv4Supported, InsertSelect) {
    // Helper to get environment variable or default
    auto getEither = [](const char* a, const char* b, const char* def) {
        const char* v = std::getenv(a);
        if (v) return std::string(v);
        v = std::getenv(b);
        if (v) return std::string(v);
        return std::string(def);
    };

    // Get connection parameters
    const std::string host = getEither("timeplus_HOST", "TIMEPLUS_HOST", "localhost");
    const std::string port = getEither("timeplus_PORT", "TIMEPLUS_PORT", "8463");
    const std::string user = getEither("timeplus_USER", "TIMEPLUS_USER", "default");
    const std::string password = getEither("timeplus_PASSWORD", "TIMEPLUS_PASSWORD", "");
    const std::string db = getEither("timeplus_DB", "TIMEPLUS_DB", "default");

    const auto opts = ClientOptions()
        .SetHost(host)
        .SetPort(static_cast<size_t>(std::stoul(port)))
        .SetUser(user)
        .SetPassword(password)
        .SetDefaultDatabase(db);

    Client client(opts);

    // Prepare test table
    client.Execute("DROP TEMPORARY STREAM IF EXISTS test_timeplus_cpp_ipv4_ut;");
    client.Execute("CREATE TEMPORARY STREAM IF NOT EXISTS test_timeplus_cpp_ipv4_ut (v4 ipv4) ENGINE = Memory");

    Block b;
    auto v4 = std::make_shared<ColumnIPv4>();
    auto ips = MakeIPv4s(); // Generate test IPv4 addresses
    for (const auto & ip : ips) {
        v4->Append(ip);
    }

    b.AppendColumn("v4", v4);
    client.Insert("test_timeplus_cpp_ipv4_ut", b);

    // Validate inserted data
    size_t row = 0;
    client.Select("SELECT v4 FROM test_timeplus_cpp_ipv4_ut", [&ips, &row](const Block& block) {
        if (block.GetRowCount() == 0) return;
        ASSERT_EQ(1U, block.GetColumnCount());
        for (size_t c = 0; c < block.GetRowCount(); ++c, ++row) {
            auto col = block[0]->As<ColumnIPv4>();
            auto got = col->At(c);
            EXPECT_EQ(ips[row].s_addr, got.s_addr);
        }
    });
    EXPECT_EQ(ips.size(), row);
}

// Test nullable IPv4 column: insert a valid and a null value, then check nullability
TEST(IPv4Supported, NullableInsertSelect) {
    // Type name must be lowercase: nullable(ipv4)
    auto getEither = [](const char* a, const char* b, const char* def) {
        const char* v = std::getenv(a);
        if (v) return std::string(v);
        v = std::getenv(b);
        if (v) return std::string(v);
        return std::string(def);
    };
    const std::string host = getEither("timeplus_HOST", "TIMEPLUS_HOST", "localhost");
    const std::string port = getEither("timeplus_PORT", "TIMEPLUS_PORT", "8463");
    const std::string user = getEither("timeplus_USER", "TIMEPLUS_USER", "default");
    const std::string password = getEither("timeplus_PASSWORD", "TIMEPLUS_PASSWORD", "");
    const std::string db = getEither("timeplus_DB", "TIMEPLUS_DB", "default");
    const auto opts = ClientOptions().SetHost(host).SetPort(static_cast<size_t>(std::stoul(port))).SetUser(user).SetPassword(password).SetDefaultDatabase(db);
    Client client(opts);
    client.Execute("DROP TEMPORARY STREAM IF EXISTS test_timeplus_cpp_ipv4_nullable;");
    client.Execute("CREATE TEMPORARY STREAM IF NOT EXISTS test_timeplus_cpp_ipv4_nullable (v4 nullable(ipv4)) ENGINE = Memory");
    Block b;
    auto v4 = std::make_shared<ColumnIPv4>();
    v4->Append("127.0.0.1"); // valid IPv4
    auto nulls = std::make_shared<ColumnUInt8>();
    nulls->Append(0); // not null
    v4->Append(0); // 0.0.0.0 as null
    nulls->Append(1); // null
    b.AppendColumn("v4", std::make_shared<ColumnNullable>(v4, nulls));
    client.Insert("test_timeplus_cpp_ipv4_nullable", b);
    size_t row = 0;
    client.Select("SELECT v4 FROM test_timeplus_cpp_ipv4_nullable", [&row](const Block& block) {
        if (block.GetRowCount() == 0) return;
        ASSERT_EQ(1U, block.GetColumnCount());
        for (size_t c = 0; c < block.GetRowCount(); ++c, ++row) {
            auto col = block[0]->As<ColumnNullable>();
            if (row == 0) {
                EXPECT_FALSE(col->IsNull(c)); // Should not be null
            } else {
                EXPECT_TRUE(col->IsNull(c)); // Should be null
            }
        }
    });
    EXPECT_EQ(2U, row);
}

// Test IPv4 boundary values: minimum and maximum
TEST(IPv4Supported, BoundaryValues) {
    auto getEither = [](const char* a, const char* b, const char* def) {
        const char* v = std::getenv(a);
        if (v) return std::string(v);
        v = std::getenv(b);
        if (v) return std::string(v);
        return std::string(def);
    };
    const std::string host = getEither("timeplus_HOST", "TIMEPLUS_HOST", "localhost");
    const std::string port = getEither("timeplus_PORT", "TIMEPLUS_PORT", "8463");
    const std::string user = getEither("timeplus_USER", "TIMEPLUS_USER", "default");
    const std::string password = getEither("timeplus_PASSWORD", "TIMEPLUS_PASSWORD", "");
    const std::string db = getEither("timeplus_DB", "TIMEPLUS_DB", "default");
    const auto opts = ClientOptions().SetHost(host).SetPort(static_cast<size_t>(std::stoul(port))).SetUser(user).SetPassword(password).SetDefaultDatabase(db);
    Client client(opts);
    client.Execute("DROP TEMPORARY STREAM IF EXISTS test_timeplus_cpp_ipv4_boundary;");
    client.Execute("CREATE TEMPORARY STREAM IF NOT EXISTS test_timeplus_cpp_ipv4_boundary (v4 ipv4) ENGINE = Memory");
    Block b;
    auto v4 = std::make_shared<ColumnIPv4>();
    v4->Append("0.0.0.0"); // min IPv4
    v4->Append("255.255.255.255"); // max IPv4
    b.AppendColumn("v4", v4);
    client.Insert("test_timeplus_cpp_ipv4_boundary", b);
    std::vector<std::string> expected = {"0.0.0.0", "255.255.255.255"};
    size_t row = 0;
    client.Select("SELECT v4 FROM test_timeplus_cpp_ipv4_boundary", [&row, &expected](const Block& block) {
        if (block.GetRowCount() == 0) return;
        ASSERT_EQ(1U, block.GetColumnCount());
        for (size_t c = 0; c < block.GetRowCount(); ++c, ++row) {
            auto col = block[0]->As<ColumnIPv4>();
            EXPECT_EQ(expected[row], col->AsString(c)); // Compare string representation
        }
    });
    EXPECT_EQ(expected.size(), row);
}

// Test invalid IPv4 input and overflow behavior
TEST(IPv4Supported, InvalidData) {
    // Test invalid IP strings, should throw ValidationError
    // Overflow assertion is kept, but C++ overflow will not throw
    auto v4 = std::make_shared<ColumnIPv4>();
    EXPECT_THROW(v4->Append("999.999.999.999"), ValidationError);
    EXPECT_THROW(v4->Append("abc.def.ghi.jkl"), ValidationError);
    EXPECT_THROW(v4->Append(0xFFFFFFFF + 1), std::exception); // Overflow: will not throw in C++
}

// Test type conversion between IPv4, string, and uint32
TEST(IPv4Supported, TypeConversion) {
    // Type names must be lowercase: string, uint32
    auto getEither = [](const char* a, const char* b, const char* def) {
        const char* v = std::getenv(a);
        if (v) return std::string(v);
        v = std::getenv(b);
        if (v) return std::string(v);
        return std::string(def);
    };
    const std::string host = getEither("timeplus_HOST", "TIMEPLUS_HOST", "localhost");
    const std::string port = getEither("timeplus_PORT", "TIMEPLUS_PORT", "8463");
    const std::string user = getEither("timeplus_USER", "TIMEPLUS_USER", "default");
    const std::string password = getEither("timeplus_PASSWORD", "TIMEPLUS_PASSWORD", "");
    const std::string db = getEither("timeplus_DB", "TIMEPLUS_DB", "default");
    const auto opts = ClientOptions().SetHost(host).SetPort(static_cast<size_t>(std::stoul(port))).SetUser(user).SetPassword(password).SetDefaultDatabase(db);
    Client client(opts);
    client.Execute("DROP TEMPORARY STREAM IF EXISTS test_timeplus_cpp_ipv4_conv;");
    client.Execute("CREATE TEMPORARY STREAM IF NOT EXISTS test_timeplus_cpp_ipv4_conv (v4 ipv4, v4str string, v4int uint32) ENGINE = Memory");
    Block b;
    auto v4 = std::make_shared<ColumnIPv4>();
    auto v4str = std::make_shared<ColumnString>();
    auto v4int = std::make_shared<ColumnUInt32>();
    v4->Append("127.0.0.1");
    v4str->Append("127.0.0.1");
    v4int->Append(2130706433u); // 127.0.0.1 as uint32
    b.AppendColumn("v4", v4);
    b.AppendColumn("v4str", v4str);
    b.AppendColumn("v4int", v4int);
    client.Insert("test_timeplus_cpp_ipv4_conv", b);
    size_t row = 0;
    client.Select("SELECT v4, v4str, v4int FROM test_timeplus_cpp_ipv4_conv", [&row](const Block& block) {
        if (block.GetRowCount() == 0) return;
        ASSERT_EQ(3U, block.GetColumnCount());
        for (size_t c = 0; c < block.GetRowCount(); ++c, ++row) {
            auto col = block[0]->As<ColumnIPv4>();
            auto colstr = block[1]->As<ColumnString>();
            auto colint = block[2]->As<ColumnUInt32>();
            EXPECT_EQ(col->AsString(c), (*colstr)[c]); // IPv4 <-> string
            in_addr addr = col->At(c);
            EXPECT_EQ(ntohl(addr.s_addr), (*colint)[c]); // IPv4 <-> uint32
        }
    });
    EXPECT_EQ(1U, row);
}
