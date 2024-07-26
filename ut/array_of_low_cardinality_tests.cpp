#include <gtest/gtest.h>
#include <algorithm>
#include <iterator>
#include <vector>

#include <timeplus/columns/array.h>
#include <timeplus/columns/string.h>
#include <timeplus/columns/lowcardinality.h>
#include "timeplus/block.h"
#include "timeplus/client.h"
#include "utils.h"
#include "timeplus/base/buffer.h"
#include "timeplus/base/output.h"

namespace
{
using namespace timeplus;
}

std::shared_ptr<ColumnArray> buildTestColumn(const std::vector<std::vector<std::string>>& rows) {
    auto arrayColumn = std::make_shared<ColumnArray>(std::make_shared<ColumnLowCardinalityT<ColumnString>>());

    for (const auto& row : rows) {
        auto column = std::make_shared<ColumnLowCardinalityT<ColumnString>>();

        for (const auto& string : row) {
            column->Append(string);
        }

        arrayColumn->AppendAsColumn(column);
    }

    return arrayColumn;
}

TEST(ArrayOfLowCardinality, Serialization) {
    const auto inputColumn = buildTestColumn({
        { "aa", "bb" },
        { "cc" }
    });

    // The serialization data was extracted from a successful insert.
    // Since we are setting a different index type in timeplus-cpp, it's expected to have different indexes.
    const std::vector<uint8_t> expectedSerialization {
        0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x02, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x61, 0x61,
        0x02, 0x62, 0x62, 0x02, 0x63, 0x63, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00,
        0x03, 0x00, 0x00, 0x00
    };

    Buffer buf;

    BufferOutput output(&buf);
    inputColumn->Save(&output);
    output.Flush();

    ASSERT_EQ(expectedSerialization, buf);
}

TEST(ArrayOfLowCardinality, InsertAndQuery) {

    const auto localHostEndpoint = ClientOptions()
                                       .SetHost(           getEnvOrDefault("TIMEPLUS_HOST",     "localhost"))
                                       .SetPort(   getEnvOrDefault<size_t>("TIMEPLUS_PORT",     "8463"))
                                       .SetUser(           getEnvOrDefault("TIMEPLUS_USER",     "default"))
                                       .SetPassword(       getEnvOrDefault("TIMEPLUS_PASSWORD", ""))
                                       .SetDefaultDatabase(getEnvOrDefault("TIMEPLUS_DB",       "default"));

    Client client(ClientOptions(localHostEndpoint)
                      .SetPingBeforeQuery(true));

    const auto testData = std::vector<std::vector<std::string>> {
        { "aa", "bb" },
        {},
        { "cc" },
        {}
    };

    auto column = buildTestColumn(testData);

    Block block;
    block.AppendColumn("arr", column);

    client.Execute("DROP TEMPORARY STREAM IF EXISTS array_lc");
    client.Execute("CREATE TEMPORARY STREAM IF NOT EXISTS array_lc (arr array(low_cardinality(string))) ENGINE = Memory");
    client.Insert("array_lc", block);

    client.Select("SELECT * FROM array_lc", [&](const Block& bl) {
        for (size_t c = 0; c < bl.GetRowCount(); ++c) {
          auto col = bl[0]->As<ColumnArray>()->GetAsColumn(c);
          for (size_t i = 0; i < col->Size(); ++i) {
              if (auto string_column = col->As<ColumnString>()) {
                  const auto string = string_column->At(i);
                  ASSERT_EQ(testData[c][i], string);
              } else if (auto lc_string_column = col->As<ColumnLowCardinalityT<ColumnString>>()) {
                  const auto string = lc_string_column->At(i);
                  ASSERT_EQ(testData[c][i], string);
              } else {
                  FAIL() << "Unexpected column type: " << col->Type()->GetName();
              }
          }
        }
    });
}
