#include <gtest/gtest.h>
#include <timeplus/columns/string.h>
#include "timeplus/columns/nullable.h"
#include "timeplus/columns/lowcardinality.h"
#include "timeplus/client.h"
#include "utils.h"
#include "timeplus/base/wire_format.h"
#include <timeplus/base/output.h>

namespace
{
using namespace timeplus;
}

static const auto localHostEndpoint = ClientOptions()
                                   .SetHost(           getEnvOrDefault("TIMEPLUS_HOST",     "localhost"))
                                   .SetPort(   getEnvOrDefault<size_t>("TIMEPLUS_PORT",     "8463"))
                                   .SetUser(           getEnvOrDefault("TIMEPLUS_USER",     "default"))
                                   .SetPassword(       getEnvOrDefault("TIMEPLUS_PASSWORD", ""))
                                   .SetDefaultDatabase(getEnvOrDefault("TIMEPLUS_DB",       "default"));


ColumnRef buildTestColumn(const std::vector<std::string>& rowsData, const std::vector<uint8_t>& nulls) {
    auto stringColumn = std::make_shared<ColumnString>(rowsData);
    auto nullsColumn = std::make_shared<ColumnUInt8>(nulls);
    auto lowCardinalityColumn = std::make_shared<ColumnLowCardinality>(
        std::make_shared<ColumnNullable>(stringColumn, nullsColumn)
    );

    return lowCardinalityColumn;
}

void createTable(Client& client) {
    client.Execute("DROP TEMPORARY STREAM IF EXISTS lc_of_nullable");
    client.Execute("CREATE TEMPORARY STREAM IF NOT EXISTS lc_of_nullable (words low_cardinality(nullable(string))) ENGINE = Memory");
}

TEST(LowCardinalityOfNullable, InsertAndQuery) {
    const auto rowsData = std::vector<std::string> {
        "eminem",
        "",
        "tupac",
        "shady",
        "fifty",
        "dre",
        "",
        "cube"
    };

    const auto nulls = std::vector<uint8_t> {
        false, false, true, false, true, true, false, false
    };

    auto column = buildTestColumn(rowsData, nulls);

    Block block;
    block.AppendColumn("words", column);

    Client client(ClientOptions(localHostEndpoint)
                             .SetBakcwardCompatibilityFeatureLowCardinalityAsWrappedColumn(false)
                             .SetPingBeforeQuery(true));

    createTable(client);

    client.Insert("lc_of_nullable", block);

    client.Select("SELECT * FROM lc_of_nullable", [&](const Block& bl) {
        for (size_t row = 0; row < bl.GetRowCount(); row++) {
            auto lc_col = bl[0]->As<ColumnLowCardinality>();
            auto item = lc_col->GetItem(row);

            if (nulls[row]) {
                ASSERT_EQ(Type::Code::Void, item.type);
            } else {
                ASSERT_EQ(rowsData[row], item.get<std::string_view>());
            }
        }
    });
}

TEST(LowCardinalityOfNullable, InsertAndQueryOneRow) {
    const auto rowsData = std::vector<std::string> {
        "eminem"
    };

    const auto nulls = std::vector<uint8_t> {
        false
    };

    auto column = buildTestColumn(rowsData, nulls);

    Block block;
    block.AppendColumn("words", column);

    Client client(ClientOptions(localHostEndpoint)
                             .SetBakcwardCompatibilityFeatureLowCardinalityAsWrappedColumn(false)
                             .SetPingBeforeQuery(true));

    createTable(client);

    client.Insert("lc_of_nullable", block);

    client.Select("SELECT * FROM lc_of_nullable", [&](const Block& bl) {
        for (size_t row = 0; row < bl.GetRowCount(); row++) {
            auto lc_col = bl[0]->As<ColumnLowCardinality>();
            auto item = lc_col->GetItem(row);

            if (nulls[row]) {
                ASSERT_EQ(Type::Code::Void, item.type);
            } else {
                ASSERT_EQ(rowsData[row], item.get<std::string_view>());
            }
        }
    });
}


TEST(LowCardinalityOfNullable, InsertAndQueryEmpty) {
    auto column = buildTestColumn({}, {});

    Block block;
    block.AppendColumn("words", column);

    Client client(ClientOptions(localHostEndpoint)
            .SetBakcwardCompatibilityFeatureLowCardinalityAsWrappedColumn(false)
            .SetPingBeforeQuery(true));

    createTable(client);

    EXPECT_NO_THROW(client.Insert("lc_of_nullable", block));

    client.Select("SELECT * FROM lc_of_nullable", [&](const Block& bl) {
        ASSERT_EQ(bl.GetRowCount(), 0u);
    });
}

TEST(LowCardinalityOfNullable, ThrowOnBackwardsCompatibleLCColumn) {
    auto column = buildTestColumn({}, {});

    Block block;
    block.AppendColumn("words", column);

    Client client(ClientOptions(localHostEndpoint)
            .SetPingBeforeQuery(true)
            .SetBakcwardCompatibilityFeatureLowCardinalityAsWrappedColumn(true));

    createTable(client);

    EXPECT_THROW(client.Insert("lc_of_nullable", block), UnimplementedError);

    client.Select("SELECT * FROM lc_of_nullable", [&](const Block& bl) {
        ASSERT_EQ(bl.GetRowCount(), 0u);
    });
}
