#include <timeplus/timeplus.h>

#include <chrono>
#include <iomanip>
#include <iostream>

using namespace timeplus;

const size_t INSERT_BLOCKS = 100'000;

const std::vector<std::pair<std::string, uint16_t>> HOST_PORTS = {
    /// Single instance
    {"localhost", 8463},
    /// Cluster nodes
    {"localhost", 18463},
    {"localhost", 28463},
    {"localhost", 38463},
};

void prepareTable() {
    ClientOptions options;
    for (const auto& [host, port] : HOST_PORTS) {
        options.endpoints.push_back({host, port});
    }

    Client client{options};
    client.Execute("DROP STREAM IF EXISTS insert_test;");
    client.Execute("CREATE STREAM insert_test (i uint64, s string);");
}

auto timestamp() {
    auto now = std::chrono::system_clock::now();
    std::time_t now_time = std::chrono::system_clock::to_time_t(now);
    std::tm* local_time = std::localtime(&now_time);
    return std::put_time(local_time, "%Y-%m-%d %H:%M:%S");
}

int main() {
    prepareTable();

    TimeplusConfig config;
    for (const auto& [host, port] : HOST_PORTS) {
        config.client_options.endpoints.push_back({host, port});
    }
    config.max_connections = 1;
    config.max_retries = 5;
    config.task_executors = 0;
    Timeplus tp{std::move(config)};

    auto block = std::make_shared<Block>();
    auto col_i = std::make_shared<ColumnUInt64>(std::vector<uint64_t>{5, 7, 4, 8});
    auto col_s = std::make_shared<ColumnString>(
        std::vector<std::string>{"Before my bed, the moon is bright,", "I think that it is frost on the ground.",
                                 "I raise my head to gaze at the bright moon,", "And lower it to think of my hometown."});
    block->AppendColumn("i", col_i);
    block->AppendColumn("s", col_s);

    auto start_time = std::chrono::high_resolution_clock::now();
    auto last_time = start_time;
    for (size_t i = 1; i <= INSERT_BLOCKS; ++i) {
        while (true) {
            try {
                tp.Insert("insert_test", block);
                break;
            } catch (const std::exception& ex) {
                std::cout << timestamp() << "\t Failed to insert block " << i << " : " << ex.what() << std::endl;
            }
        }

        if (i % 1000 == 0) {
            auto current_time = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double> elapsed = current_time - last_time;
            last_time = current_time;
            std::cout << "[" << timestamp() << "]\t" << i << " blocks inserted\telapsed = " << elapsed.count()
                      << " sec\teps = " << 1000.0 * block->GetRowCount() / elapsed.count() << std::endl;
        }
    }

    auto current_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = current_time - start_time;
    std::cout << "\nInsert Done. Total Events = " << INSERT_BLOCKS * block->GetRowCount() << " Total Time = " << elapsed.count() << " sec"
              << std::endl;

    return 0;
}
