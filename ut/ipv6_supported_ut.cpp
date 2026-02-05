#include <timeplus/client.h>
#include <timeplus/columns/ip6.h>

#include "ut/utils.h"
#include "ut/value_generators.h"
#include <cstring>
#include <gtest/gtest.h>

using namespace timeplus;

// Test inserting and selecting IPv6 addresses
TEST(IPv6Supported, InsertSelect) {
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
    client.Execute("DROP TEMPORARY STREAM IF EXISTS test_timeplus_cpp_ipv6_ut;");
    client.Execute("CREATE TEMPORARY STREAM IF NOT EXISTS test_timeplus_cpp_ipv6_ut (v6 ipv6) ENGINE = Memory");

    Block b;
    auto v6 = std::make_shared<ColumnIPv6>();
    auto ips6 = MakeIPv6s(); // Generate test IPv6 addresses
    for (const auto & ip : ips6) {
        v6->Append(ip);
    }

    b.AppendColumn("v6", v6);
    client.Insert("test_timeplus_cpp_ipv6_ut", b);

    // Validate inserted data
    size_t row = 0;
    client.Select("SELECT v6 FROM test_timeplus_cpp_ipv6_ut", [&ips6, &row](const Block& block) {
        if (block.GetRowCount() == 0) return;
        ASSERT_EQ(1U, block.GetColumnCount());
        for (size_t c = 0; c < block.GetRowCount(); ++c, ++row) {
            auto col = block[0]->As<ColumnIPv6>();
            auto got = col->At(c);
            EXPECT_EQ(0, std::memcmp(&ips6[row], &got, sizeof(in6_addr)));
        }
    });
    EXPECT_EQ(ips6.size(), row);
}

// Test nullable IPv6 column: insert a valid and a null value, then check nullability
TEST(IPv6Supported, NullableInsertSelect) {
    // Type name must be lowercase: nullable(ipv6)
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
    client.Execute("DROP TEMPORARY STREAM IF EXISTS test_timeplus_cpp_ipv6_nullable;");
    client.Execute("CREATE TEMPORARY STREAM IF NOT EXISTS test_timeplus_cpp_ipv6_nullable (v6 nullable(ipv6)) ENGINE = Memory");
    Block b;
    auto v6 = std::make_shared<ColumnIPv6>();
    v6->Append("::1"); // valid IPv6
    auto nulls = std::make_shared<ColumnUInt8>();
    nulls->Append(0); // not null
    v6->Append("::"); // :: as null (all zero IPv6)
    nulls->Append(1); // null
    b.AppendColumn("v6", std::make_shared<ColumnNullable>(v6, nulls));
    client.Insert("test_timeplus_cpp_ipv6_nullable", b);
    size_t row = 0;
    client.Select("SELECT v6 FROM test_timeplus_cpp_ipv6_nullable", [&row](const Block& block) {
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

// Test invalid IPv6 input
TEST(IPv6Supported, InvalidData) {
    // Test invalid IPv6 strings, should throw ValidationError
    auto v6 = std::make_shared<ColumnIPv6>();
    EXPECT_THROW(v6->Append("gggg:gggg:gggg:gggg:gggg:gggg:gggg:gggg"), ValidationError);
    EXPECT_THROW(v6->Append("not_an_ip"), ValidationError);
}

// Test type conversion between IPv6 and string
TEST(IPv6Supported, TypeConversion) {
    // Type name must be lowercase: string
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
    client.Execute("DROP TEMPORARY STREAM IF EXISTS test_timeplus_cpp_ipv6_conv;");
    client.Execute("CREATE TEMPORARY STREAM IF NOT EXISTS test_timeplus_cpp_ipv6_conv (v6 ipv6, v6str string) ENGINE = Memory");
    Block b;
    auto v6 = std::make_shared<ColumnIPv6>();
    auto v6str = std::make_shared<ColumnString>();
    v6->Append("::1");
    v6str->Append("::1");
    b.AppendColumn("v6", v6);
    b.AppendColumn("v6str", v6str);
    client.Insert("test_timeplus_cpp_ipv6_conv", b);
    size_t row = 0;
    client.Select("SELECT v6, v6str FROM test_timeplus_cpp_ipv6_conv", [&row](const Block& block) {
        if (block.GetRowCount() == 0) return;
        ASSERT_EQ(2U, block.GetColumnCount());
        for (size_t c = 0; c < block.GetRowCount(); ++c, ++row) {
            auto col = block[0]->As<ColumnIPv6>();
            auto colstr = block[1]->As<ColumnString>();
            EXPECT_EQ(col->AsString(c), (*colstr)[c]); // IPv6 <-> string
        }
    });
    EXPECT_EQ(1U, row);
}
