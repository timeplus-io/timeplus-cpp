#include <clickhouse/client.h>
#include <clickhouse/error_codes.h>
#include <clickhouse/types/type_parser.h>
#include <clickhouse/base/socket.h>

#include <ut/utils.h>

#include <stdexcept>
#include <iostream>
#include <cmath>
#include <thread>
#include <iostream>
#if defined(_MSC_VER)
#   pragma warning(disable : 4996)
#endif

using namespace clickhouse;

inline void InsertExample(Client& client) {
    /// Create a table.
     client.Execute("CREATE STREAM IF NOT EXISTS test_insert (id uint64, str string)");

    /// Insert some values.
    {
        const size_t ITEMS_COUNT = 1000;


        auto start_of_insert = std::chrono::high_resolution_clock::now();

        for (size_t i = 0; i < ITEMS_COUNT; ++i) {
            Block block;

            auto id = std::make_shared<ColumnUInt64>();
            auto s = std::make_shared<ColumnString>();

            id->Append(static_cast<std::uint64_t>(i + 1));
            s->Append(std::to_string(i + 1));

            block.AppendColumn("id", id);
            block.AppendColumn("str", s);
            client.Insert("test_insert", block);

        }
        auto end_of_insert = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_of_insert - start_of_insert);
        std::cout << "insert time: " << duration.count() << " milliseconds." << std::endl;
    }

    /// Select values inserted in the previous step.
    // client.Select("SELECT id, s FROM test_insert", [](const Block& block)
    //     {
    //         for (Block::Iterator bi(block); bi.IsValid(); bi.Next()) {
    //             std::cout << bi.Name() << " ";
    //         }
    //         std::cout << std::endl;

    //         for (size_t i = 0; i < block.GetRowCount(); ++i) {
    //             std::cout << (*block[0]->As<ColumnUInt64>())[i] << " "
    //                       << (*block[1]->As<ColumnEnum16>()).NameAt(i) << "\n";
    //         }
    //     }
    // );

    /// Delete table.
    // client.Execute("DROP STREAM test_insert");
}



int main()
{
    /// Initialize client connection.
    try {
        const auto localHostEndpoint = ClientOptions()
                .SetHost(   getEnvOrDefault("TIMEPLUS_HOST",     "localhost"))
                .SetPort(   getEnvOrDefault<size_t>("TIMEPLUS_PORT",     "8463"));

        {
            Client client(ClientOptions(localHostEndpoint)
                    .SetPingBeforeQuery(true));
            InsertExample(client);
            // std::cout << "current endpoint : " <<  client.GetCurrentEndpoint().value().host << "\n";
        }
    } catch (const std::exception& e) {
        std::cerr << "exception : " << e.what() << std::endl;
    }


    return 0;
}