#include <timeplus/block.h>
#include <timeplus/client.h>
#include <timeplus/columns/numeric.h>
#include <timeplus/error_codes.h>
#include <timeplus/inserter.h>

#include <iostream>
#include <string>
#include <thread>


using namespace timeplus;

Block createBlock(const std::string & dataID, size_t rows)
{
    Block block;

    auto id = std::make_shared<ColumnString>();
    auto index = std::make_shared<ColumnUInt64>();

    for (size_t i = 0; i < rows; ++i)
    {
        id->Append(dataID);
        index->Append(i);
    }

    block.AppendColumn("data_id", id);
    block.AppendColumn("number", index);

    return block;
}

void printTable(Client & client, const std::string & table_name)
{
    std::cout << "data_id\tnumber" << std::endl;
    client.Select("SELECT data_id, number FROM table(" + table_name + ")", [](const Block & block) {
        for (size_t i = 0; i < block.GetRowCount(); ++i)
            std::cout << block[0]->As<ColumnString>()->At(i) << "\t" << block[1]->As<ColumnUInt64>()->At(i) << "\n";
    });
}

std::string toString(const InsertResult & result)
{
    return "id = " + result.id + " result = " + (result.err_code == ErrorCodes::OK ? "ok" : result.err_msg);
}

void handleResult(const InsertResult & result)
{
    std::cout << "Data insert completed: " << toString(result) << std::endl;
}

int main()
{
    std::string table_name = "default.example_inserter";

    ClientOptions client_options;
    client_options.SetHost("localhost").SetPort(8463);
    Client client{client_options};

    Inserter::Options inserter_options{client_options, /*idempotent_=*/true};
    Inserter inserter{inserter_options};

    client.Execute("CREATE STREAM IF NOT EXISTS " + table_name + "(data_id string, number uint64)");

    std::cout << "Idempotent insert block-0 twice." << std::endl;

    auto id = "data-0";
    auto block0 = createBlock(id, 2);
    InsertData data0{id, true, table_name, std::move(block0)};

    auto insert_result = inserter.InsertSync(data0);
    handleResult(insert_result);

    insert_result = inserter.InsertSync(data0);
    handleResult(insert_result);

    std::cout << std::endl;

    std::cout << "Trigger three asynchronous insert." << std::endl;
    for (int i = 1; i <= 3; ++i)
    {
        auto dataID = "data-" + std::to_string(i);
        auto block = createBlock(dataID, 2);
        InsertData data{dataID, /*idempotent_=*/true, table_name, std::move(block)};
        inserter.Insert(std::move(data), handleResult);
    }
    std::cout << "Asynchronous insert triggered." << std::endl << std::endl;

    std::cout << "Sleep..." << std::endl;
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(3s);

    std::cout << "Table after insert: " << table_name << std::endl;
    printTable(client, table_name);

    return 0;
}
