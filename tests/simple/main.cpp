#include <timeplus/client.h>
#include <timeplus/error_codes.h>
#include <timeplus/types/type_parser.h>
#include <timeplus/base/socket.h>

#include <stdexcept>
#include <iostream>
#include <cmath>
#include <thread>
#include <iostream>
#if defined(_MSC_VER)
#   pragma warning(disable : 4996)
#endif

using namespace timeplus;

void InsertTask(const ClientOptions& client_options, const size_t start_index, const size_t end_index) {
    try {
        Client client(client_options);

        // Create a table for each thread if necessary (optional, depends on the use case)
        client.Execute("DROP STREAM IF EXISTS test_insert");
        client.Execute("CREATE STREAM IF NOT EXISTS test_insert (id uint64, str string, d256 decimal256(38))");

        for (size_t i = start_index; i < end_index; ++i) {
            Block block;

            auto id = std::make_shared<ColumnUInt64>();
            auto s = std::make_shared<ColumnString>();
            auto d256 = std::make_shared<ColumnDecimal>(76,38);

            id->Append(static_cast<std::uint64_t>(i + 1));
            s->Append(std::to_string(i + 1));
            d256->Append(static_cast<std::string>("9999999999999999999999999999.9999999999999999999999"));

            block.AppendColumn("id", id);
            block.AppendColumn("str", s);
            block.AppendColumn("d256", d256);
            client.Insert("test_insert", block);
        }
    } catch (const std::exception& e) {
        std::cerr << "Thread " << std::this_thread::get_id() << " encountered an error: " << e.what() << std::endl;
    }
}

int main() {
    const size_t TOTAL_ITEMS = 10;
    const size_t THREADS_COUNT = 2;
    const size_t ITEMS_PER_THREAD = TOTAL_ITEMS / THREADS_COUNT;
    std::vector<std::thread> threads;

    ClientOptions client_options;
    client_options
        .SetHost(("localhost"))
        .SetPort(std::stoi("8463"))
        .SetPingBeforeQuery(true);

    // auto start_of_insert = std::chrono::high_resolution_clock::now();

    for (size_t i = 0; i < THREADS_COUNT; ++i) {
        size_t start_index = i * ITEMS_PER_THREAD;
        size_t end_index = (i == THREADS_COUNT - 1) ? TOTAL_ITEMS : (start_index + ITEMS_PER_THREAD);
        threads.emplace_back(InsertTask, client_options, start_index, end_index);
    }

    auto start_of_insert = std::chrono::high_resolution_clock::now();
    for (auto& th : threads) {
        th.join();
    }

    auto end_of_insert = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_of_insert - start_of_insert);
    std::cout << "Total insert time with " << THREADS_COUNT << " threads: " << duration.count() << " milliseconds." << std::endl;

    return 0;
}
