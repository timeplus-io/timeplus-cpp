#include <timeplus/blocking_queue.h>
#include <timeplus/timeplus.h>

#include <atomic>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <thread>

using namespace timeplus;

const size_t INSERT_BLOCKS = 100'000;
const size_t BLOCKS_PER_BATCH = 1000;

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
    client.Execute("DROP STREAM IF EXISTS insert_async_test;");
    client.Execute("CREATE STREAM insert_async_test (i uint64, s string);");
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
    config.max_connections = 4;
    config.max_retries = 5;
    config.task_executors = 4;
    config.task_queue_capacity = BLOCKS_PER_BATCH;  /// use large input queue to avoid deadlock on retry failure

    Timeplus tp{std::move(config)};

    auto block = std::make_shared<Block>();
    auto col_i = std::make_shared<ColumnUInt64>(std::vector<uint64_t>{5, 7, 4, 8});
    auto col_s = std::make_shared<ColumnString>(
        std::vector<std::string>{"Before my bed, the moon is bright,", "I think that it is frost on the ground.",
                                 "I raise my head to gaze at the bright moon,", "And lower it to think of my hometown."});
    block->AppendColumn("i", col_i);
    block->AppendColumn("s", col_s);

    /// Queue to store failed inserts which need to be resent.
    BlockingQueue<std::pair<size_t, BlockPtr>> insert_failure(BLOCKS_PER_BATCH);
    std::atomic<size_t> insert_success_count{0};

    auto handle_insert_result = [&insert_failure, &insert_success_count](size_t block_id, const InsertResult& result) {
        if (result.ok()) {
            insert_success_count.fetch_add(1);
        } else {
            std::cout << "[" << timestamp() << "]\t Failed to insert block: insert_id=" << block_id << " err=" << result.err_msg
                      << std::endl;
            insert_failure.emplace(block_id, result.block);
        }
    };

    auto async_insert_block = [&tp, &handle_insert_result](size_t block_id, BlockPtr block) {
        tp.InsertAsync(/*table_name=*/"insert_async_test", block, [block_id, &handle_insert_result](const BaseResult& result) {
            const auto& insert_result = static_cast<const InsertResult&>(result);
            handle_insert_result(block_id, insert_result);
        });
    };

    auto start_time = std::chrono::high_resolution_clock::now();
    auto last_time = start_time;
    for (size_t batch = 0; batch < INSERT_BLOCKS / BLOCKS_PER_BATCH; ++batch) {
        insert_success_count = 0;
        /// Insert blocks asynchronously.
        for (size_t i = 0; i < BLOCKS_PER_BATCH; ++i) {
            async_insert_block(i, block);
        }

        /// Wait for all blocks of the batch are inserted.
        while (insert_success_count.load() != BLOCKS_PER_BATCH) {
            if (!insert_failure.empty()) {
                /// Re-insert the failed blocks
                auto blocks = insert_failure.drain();
                for (auto &[i, b] : blocks) {
                    async_insert_block(i, b);
                }
            }

            std::this_thread::yield();
        }

        /// Print insert statistics of the batch.
        auto current_time = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> elapsed = current_time - last_time;
        last_time = current_time;
        std::cout << "[" << timestamp() << "]\t" << (batch + 1) * BLOCKS_PER_BATCH << " blocks inserted\telapsed = " << elapsed.count()
                  << " sec\teps = " << static_cast<double>(BLOCKS_PER_BATCH * block->GetRowCount()) / elapsed.count() << std::endl;
    }

    /// Print summary.
    auto current_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> elapsed = current_time - start_time;
    std::cout << "\nInsert Done. Total Events = " << INSERT_BLOCKS * block->GetRowCount() << " Total Time = " << elapsed.count() << " sec"
              << std::endl;

    return 0;
}
