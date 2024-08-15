#include <timeplus/timeplus.h>

#include <iostream>

using namespace timeplus;

/// Stream to insert is created with DDL:
/// `CREATE STREAM insert_examples(i uint64, v string)`
const std::string TABLE_NAME = "insert_examples";

int main() {
    TimeplusConfig config;
    config.client_options.endpoints.push_back({"localhost", 8463});
    config.max_connections = 3;
    config.max_retries = 10;
    config.wait_time_before_retry_ms = 1000;
    config.task_executors = 1;

    Timeplus tp{std::move(config)};

    auto block = std::make_shared<Block>();

    auto col_i = std::make_shared<ColumnUInt64>();
    col_i->Append(5);
    col_i->Append(7);
    block->AppendColumn("i", col_i);

    auto col_v = std::make_shared<ColumnString>();
    col_v->Append("five");
    col_v->Append("seven");
    block->AppendColumn("v", col_v);

    /// Use synchronous insert API.
    auto insert_result = tp.Insert(TABLE_NAME, block, /*idempotent_id=*/"block-1");
    if (insert_result.ok()) {
        std::cout << "Synchronous insert suceeded." << std::endl;
    } else {
        std::cout << "Synchronous insert failed: code=" << insert_result.err_code << " msg=" << insert_result.err_msg << std::endl;
    }

    /// Use asynchrounous insert API.
    std::atomic<bool> done = false;
    tp.InsertAsync(TABLE_NAME, block, /*idempotent_id=*/"block-2", [&done](const BaseResult& result) {
        const auto& async_insert_result = static_cast<const InsertResult&>(result);
        if (async_insert_result.ok()) {
            std::cout << "Asynchronous insert suceeded." << std::endl;
        } else {
            std::cout << "Asynchronous insert failed: code=" << async_insert_result.err_code << " msg=" << async_insert_result.err_msg
                      << std::endl;
        }
        done = true;
    });

    while (!done) {
    }

    return 0;
}
