#include <timeplus/blocking_queue.h>
#include <timeplus/client.h>
#include <timeplus/timeplus.h>
#include <timeplus/timeplus_config.h>

#include <chrono>
#include <iomanip>
#include <iostream>

using namespace timeplus;

/* Test stream DDL

CREATE STREAM IF NOT EXISTS insert_benchmark_test (
  Field1 string,
  Field2 string,
  Field3 int64,
  Field4 int64,
  Field5 string,
  Field6 low_cardinality(string),
  Field7 low_cardinality(string),
  Field8 string,
  Field9 low_cardinality(string),
  Field10 string,
  Field11 low_cardinality(string),
  Field12 low_cardinality(string),
  Field13 string,
  Field14 string,
  Field15 string,
  Field16 int64,
  Field17 int64,
  Field18 int64,
  Field19 int64,
  Field20 int32,
  Field21 float64,
  Field22 float64,
  Field23 float64,
  Field24 string,
  Field25 int64,
  Field26 int64,
  Field27 float64,
  Field28 float64,
  Field29 float64,
  Field30 string,
  Field31 string,
  Field32 string,
  _tp_time datetime64(3,'UTC') DEFAULT now64(3,'UTC') CODEC(DoubleDelta, LZ4),
  INDEX _tp_time_index _tp_time TYPE minmax GRANULARITY 2
)

*/

BlockPtr createBlock(size_t rows) {
    BlockPtr block_ptr = std::make_shared<Block>();
    auto& block = *block_ptr;
    block.AppendColumn("Field1", std::make_shared<ColumnString>());
    block.AppendColumn("Field2", std::make_shared<ColumnString>());
    block.AppendColumn("Field3", std::make_shared<ColumnInt64>());
    block.AppendColumn("Field4", std::make_shared<ColumnInt64>());
    block.AppendColumn("Field5", std::make_shared<ColumnString>());
    block.AppendColumn("Field6", std::make_shared<ColumnLowCardinalityT<ColumnString>>());
    block.AppendColumn("Field7", std::make_shared<ColumnLowCardinalityT<ColumnString>>());
    block.AppendColumn("Field8", std::make_shared<ColumnString>());
    block.AppendColumn("Field9", std::make_shared<ColumnLowCardinalityT<ColumnString>>());
    block.AppendColumn("Field10", std::make_shared<ColumnString>());
    block.AppendColumn("Field11", std::make_shared<ColumnLowCardinalityT<ColumnString>>());
    block.AppendColumn("Field12", std::make_shared<ColumnLowCardinalityT<ColumnString>>());
    block.AppendColumn("Field13", std::make_shared<ColumnString>());
    block.AppendColumn("Field14", std::make_shared<ColumnString>());
    block.AppendColumn("Field15", std::make_shared<ColumnString>());

    block.AppendColumn("Field16", std::make_shared<ColumnInt64>());
    block.AppendColumn("Field17", std::make_shared<ColumnInt64>());
    block.AppendColumn("Field18", std::make_shared<ColumnInt64>());
    block.AppendColumn("Field19", std::make_shared<ColumnInt64>());
    block.AppendColumn("Field20", std::make_shared<ColumnInt32>());
    block.AppendColumn("Field21", std::make_shared<ColumnFloat64>());
    block.AppendColumn("Field22", std::make_shared<ColumnFloat64>());
    block.AppendColumn("Field23", std::make_shared<ColumnFloat64>());
    block.AppendColumn("Field24", std::make_shared<ColumnString>());
    block.AppendColumn("Field25", std::make_shared<ColumnInt64>());
    block.AppendColumn("Field26", std::make_shared<ColumnInt64>());

    block.AppendColumn("Field27", std::make_shared<ColumnFloat64>());
    block.AppendColumn("Field28", std::make_shared<ColumnFloat64>());
    block.AppendColumn("Field29", std::make_shared<ColumnFloat64>());
    block.AppendColumn("Field30", std::make_shared<ColumnString>());
    block.AppendColumn("Field31", std::make_shared<ColumnString>());
    block.AppendColumn("Field32", std::make_shared<ColumnString>());

    for (size_t i = 0; i < rows; ++i) {
        size_t col = 0;
        block[col++]->As<ColumnString>()->Append("123456");
        block[col++]->As<ColumnString>()->Append("142400000");
        block[col++]->As<ColumnInt64>()->Append(20230328);
        block[col++]->As<ColumnInt64>()->Append(142400000);
        block[col++]->As<ColumnString>()->Append("123");
        block[col++]->As<ColumnLowCardinalityT<ColumnString>>()->Append("DefaultField6");
        block[col++]->As<ColumnLowCardinalityT<ColumnString>>()->Append("02001");
        block[col++]->As<ColumnString>()->Append("600001");
        block[col++]->As<ColumnLowCardinalityT<ColumnString>>()->Append("DefaultField9");
        block[col++]->As<ColumnString>()->Append("600001.SH");
        block[col++]->As<ColumnLowCardinalityT<ColumnString>>()->Append("TestLevel");
        block[col++]->As<ColumnLowCardinalityT<ColumnString>>()->Append("DefaultField12");
        block[col++]->As<ColumnString>()->Append("3");
        block[col++]->As<ColumnString>()->Append("Test_Data");
        block[col++]->As<ColumnString>()->Append("TransactionType");

        block[col++]->As<ColumnInt64>()->Append(12);
        block[col++]->As<ColumnInt64>()->Append(1243);
        block[col++]->As<ColumnInt64>()->Append(25467);
        block[col++]->As<ColumnInt64>()->Append(1);
        block[col++]->As<ColumnInt32>()->Append(1);
        block[col++]->As<ColumnFloat64>()->Append(10.56);
        block[col++]->As<ColumnFloat64>()->Append(100);
        block[col++]->As<ColumnFloat64>()->Append(234.67);
        block[col++]->As<ColumnString>()->Append("ABCD1111");
        block[col++]->As<ColumnInt64>()->Append(20230403123400000);
        block[col++]->As<ColumnInt64>()->Append(10);

        block[col++]->As<ColumnFloat64>()->Append(12.03);
        block[col++]->As<ColumnFloat64>()->Append(1.5);
        block[col++]->As<ColumnFloat64>()->Append(123.0);
        block[col++]->As<ColumnString>()->Append("20230328");
        block[col++]->As<ColumnString>()->Append("175638123");
        block[col++]->As<ColumnString>()->Append("80-12345353-213-12345");
    }

    block.RefreshRowCount();

    return block_ptr;
}

auto timestamp(const std::time_t& now_time) {
    std::tm* local_time = std::localtime(&now_time);
    return std::put_time(local_time, "%Y-%m-%d %H:%M:%S");
}

int main(int argc, char* argv[]) {
    if (argc != 4) {
        std::cerr << "Usage: " << argv[0] << " <host> <port> <batch_size>" << std::endl;
        return 1;
    }

    const std::string host = argv[1];
    const auto port = std::stoi(argv[2]);
    const auto batch_size = std::stoul(argv[3]);

    TimeplusConfig config;
    config.client_options.host = host;
    config.client_options.port = port;
    config.max_connections = 10;
    config.wait_time_before_retry_ms = 100;
    config.task_executors = 4;

    Timeplus tp{std::move(config)};

    auto block = createBlock(batch_size);

    double total_time = 0;
    double batch_elapsed = 0;

    auto start_datetime = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());

    constexpr double execution_time_sec = 7200;
    constexpr auto block_in_batch = 1000;
    int block_id = 1;
    BlockingQueue<std::tuple<std::string, BlockPtr, std::string>> queue(block_in_batch);

    std::cout << "timestamp,batch_time,eps" << std::endl;
    while (true) {
        auto start = std::chrono::high_resolution_clock::now();
        for (int i = 0; i < block_in_batch; ++i) {
            queue.emplace("mds_sh_transaction", block, std::to_string(block_id++));
        }
        std::atomic_int ongoing = queue.size();
        while (ongoing > 0) {
            auto maybe_entry = queue.take(1000);
            if (!maybe_entry.has_value()) continue;
            auto [table, block, id] = std::move(maybe_entry).value();
            tp.InsertAsync(std::move(table), std::move(block), std::move(id), [&queue, &ongoing](const BaseResult& result) {
                const auto& insert_result = static_cast<const InsertResult&>(result);
                if (result.ok()) {
                    ongoing--;
                } else {
                    queue.emplace(insert_result.table_name, insert_result.block, insert_result.idempotent_id);
                }
            });
        }
        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> elapsed = end - start;
        total_time += elapsed.count();
        batch_elapsed += elapsed.count();
        std::cout << std::fixed << total_time << "," << batch_elapsed << "," << block->GetRowCount() * block_in_batch / batch_elapsed
                  << std::endl;
        batch_elapsed = 0;
        if (total_time >= execution_time_sec) {
            break;
        }
    }

    auto end_datetime = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::cout << std::endl;
    std::cout << "Start: " << timestamp(start_datetime) << std::endl;
    std::cout << "End: " << timestamp(end_datetime) << std::endl;
    std::cout << "Elapsed: " << total_time << std::endl;
    std::cout << "Blocks: " << block_id - 1 << std::endl;
    std::cout << "EPS: " << (block_id - 1) * block->GetRowCount() / total_time << std::endl;

    return 0;
}
