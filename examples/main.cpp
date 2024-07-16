#include <iostream>
#include <clickhouse/client.h>

using namespace clickhouse;

int main()
{
    /// Initialize client connection.
    Client client(ClientOptions().SetHost("localhost").SetPort(8463));

    /// Create a table.
    client.Execute("CREATE STREAM IF NOT EXISTS default.numbers (id uint64, name string) ENGINE = Memory");

    /// Insert some values.
    {
        Block block;

        auto id = std::make_shared<ColumnUInt64>();
        id->Append(1);
        id->Append(7);

        auto name = std::make_shared<ColumnString>();
        name->Append("one");
        name->Append("seven");

        block.AppendColumn("id"  , id);
        block.AppendColumn("name", name);

        client.Insert("default.numbers", block);
    }

    /// Select values inserted in the previous step.
    client.Select("SELECT id, name FROM default.numbers", [] (const Block& block)
        {
            for (size_t i = 0; i < block.GetRowCount(); ++i) {
                std::cout << block[0]->As<ColumnUInt64>()->At(i) << " "
                          << block[1]->As<ColumnString>()->At(i) << "\n";
            }
        }
    );

    /// Delete table.
    // client.Execute("DROP STREAM default.numbers");

    return 0;
}
