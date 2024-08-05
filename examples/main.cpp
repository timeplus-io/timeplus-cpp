#include <timeplus/client.h>
#include <timeplus/error_codes.h>
#include <timeplus/types/type_parser.h>
#include <timeplus/base/socket.h>

#include <ut/utils.h>

#include <stdexcept>
#include <iostream>
#include <cmath>
#include <thread>
#include <iostream>
#if defined(_MSC_VER)
#   pragma warning(disable : 4996)
#endif

using namespace timeplus;

void createAndSelect(Client& client) {

    /// Create a table.
    client.Execute("CREATE STREAM IF NOT EXISTS default.numbers (id uint64, name string)");

    /// Select values inserted in the previous step.
    client.Select("SELECT id, name, _tp_time FROM default.numbers", [] (const Block& block)
        {
            for (size_t i = 0; i < block.GetRowCount(); ++i) {
                std::cout << block[0]->As<ColumnUInt64>()->At(i) << " "
                          << block[1]->As<ColumnString>()->At(i) << "\n";
                auto colt = block[2]->As<ColumnDateTime64>();
                std::time_t t = colt->At(i);
                std::cout << formatTimestamp(t, 3);
                std::cout << colt->Timezone() << std::endl;
            }
        }
    );

}

void insertStream(Client& client) {

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
}

void dropStream(Client& client) {

    /// Delete stream.
    client.Execute("DROP STREAM IF EXISTS default.numbers");
}

void (*functionPointers[])(Client&) = {createAndSelect, insertStream, dropStream};

int main(int argc, char* argv[]) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <function_number>" << std::endl;
        return 1;
    }

    int functionNumber = std::stoi(argv[1]);

    if (functionNumber < 1 || functionNumber > 3) {
        std::cerr << "Invalid function number. Please enter: "<<"\n"
                  << "1 (create stream)" <<"\n"
                  << "2 (insert into stream)" <<"\n"
                  << "3 (delete stream)" <<std::endl;
        return 1;
    }

    /// Initialize client connection.
    Client client(ClientOptions().SetHost("localhost").SetPort(8463));

    functionPointers[functionNumber - 1](client);

    return 0;
}
