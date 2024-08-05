#include <timeplus/client.h>

#include <chrono>
#include <iostream>
#include <thread>

using namespace timeplus;

void createTable(Client& client) {
    client.Execute("DROP STREAM mds_sh_transaction");

    client.Execute(R"(
CREATE STREAM IF NOT EXISTS mds_sh_transaction (
  MDRecordID string,
  MDReportID string,
  MDDate int64,
  MDTime int64,
  MDStreamID string,
  SecurityType low_cardinality(string),
  SecuritySubType low_cardinality(string),
  SecurityID string,
  SecurityIDSource low_cardinality(string),
  Symbol string,
  MDLevel low_cardinality(string),
  MDChannel low_cardinality(string),
  TradingPhaseCode string,
  SwitchStatus string,
  MDRecordType string,
  TradeIndex int64,
  TradeBuyNo int64,
  TradeSellNo int64,
  TradeType int64,
  TradeBSFlag int32,
  TradePrice float64,
  TradeQty float64,
  TradeMoney float64,
  HTSCSecurityID string,
  ReceiveDateTime int64,
  NumTrades int64,
  TradeCleanPrice float64,
  AccruedInterestAmt float64,
  TradeDirtyPrice float64,
  DealDate string,
  DealTime string,
  DealNumber string,
  _tp_time datetime64(3,'UTC') DEFAULT now64(3,'UTC') CODEC(DoubleDelta, LZ4),
  INDEX _tp_time_index _tp_time TYPE minmax GRANULARITY 2
)
Engine = Stream(1, 3, rand())
PARTITION BY to_start_of_hour(_tp_time)
SETTINGS index_granularity = 8192, logstore_retention_bytes = -1, logstore_retention_ms = 86400000;
                   )");

    using namespace std::chrono_literals;
    std::this_thread::sleep_for(2s);
}

Block createBlock(size_t rows) {
    Block block;
    block.AppendColumn("MDRecordID", std::make_shared<ColumnString>());
    block.AppendColumn("MDReportID", std::make_shared<ColumnString>());
    block.AppendColumn("MDDate", std::make_shared<ColumnInt64>());
    block.AppendColumn("MDTime", std::make_shared<ColumnInt64>());
    block.AppendColumn("MDStreamID", std::make_shared<ColumnString>());
    block.AppendColumn("SecurityType", std::make_shared<ColumnLowCardinalityT<ColumnString>>());
    block.AppendColumn("SecuritySubType", std::make_shared<ColumnLowCardinalityT<ColumnString>>());
    block.AppendColumn("SecurityID", std::make_shared<ColumnString>());
    block.AppendColumn("SecurityIDSource", std::make_shared<ColumnLowCardinalityT<ColumnString>>());
    block.AppendColumn("Symbol", std::make_shared<ColumnString>());
    block.AppendColumn("MDLevel", std::make_shared<ColumnLowCardinalityT<ColumnString>>());
    block.AppendColumn("MDChannel", std::make_shared<ColumnLowCardinalityT<ColumnString>>());
    block.AppendColumn("TradingPhaseCode", std::make_shared<ColumnString>());
    block.AppendColumn("SwitchStatus", std::make_shared<ColumnString>());
    block.AppendColumn("MDRecordType", std::make_shared<ColumnString>());

    block.AppendColumn("TradeIndex", std::make_shared<ColumnInt64>());
    block.AppendColumn("TradeBuyNo", std::make_shared<ColumnInt64>());
    block.AppendColumn("TradeSellNo", std::make_shared<ColumnInt64>());
    block.AppendColumn("TradeType", std::make_shared<ColumnInt64>());
    block.AppendColumn("TradeBSFlag", std::make_shared<ColumnInt32>());
    block.AppendColumn("TradePrice", std::make_shared<ColumnFloat64>());
    block.AppendColumn("TradeQty", std::make_shared<ColumnFloat64>());
    block.AppendColumn("TradeMoney", std::make_shared<ColumnFloat64>());
    block.AppendColumn("HTSCSecurityID", std::make_shared<ColumnString>());
    block.AppendColumn("ReceiveDateTime", std::make_shared<ColumnInt64>());
    block.AppendColumn("NumTrades", std::make_shared<ColumnInt64>());

    block.AppendColumn("TradeCleanPrice", std::make_shared<ColumnFloat64>());
    block.AppendColumn("AccruedInterestAmt", std::make_shared<ColumnFloat64>());
    block.AppendColumn("TradeDirtyPrice", std::make_shared<ColumnFloat64>());
    block.AppendColumn("DealDate", std::make_shared<ColumnString>());
    block.AppendColumn("DealTime", std::make_shared<ColumnString>());
    block.AppendColumn("DealNumber", std::make_shared<ColumnString>());

    for (size_t i = 0; i < rows; ++i) {
        size_t col = 0;
        block[col++]->As<ColumnString>()->Append("123456");
        block[col++]->As<ColumnString>()->Append("142400000");
        block[col++]->As<ColumnInt64>()->Append(20230328);
        block[col++]->As<ColumnInt64>()->Append(142400000);
        block[col++]->As<ColumnString>()->Append("123");
        block[col++]->As<ColumnLowCardinalityT<ColumnString>>()->Append("DefaultSecurityType");
        block[col++]->As<ColumnLowCardinalityT<ColumnString>>()->Append("02001");
        block[col++]->As<ColumnString>()->Append("600001");
        block[col++]->As<ColumnLowCardinalityT<ColumnString>>()->Append("DefaultSecurityIDSource");
        block[col++]->As<ColumnString>()->Append("600001.SH");
        block[col++]->As<ColumnLowCardinalityT<ColumnString>>()->Append("LevelOne");
        block[col++]->As<ColumnLowCardinalityT<ColumnString>>()->Append("DefaultMDChannel");
        block[col++]->As<ColumnString>()->Append("3");
        block[col++]->As<ColumnString>()->Append("switch");
        block[col++]->As<ColumnString>()->Append("TransactionType");

        block[col++]->As<ColumnInt64>()->Append(12);
        block[col++]->As<ColumnInt64>()->Append(1243);
        block[col++]->As<ColumnInt64>()->Append(25467);
        block[col++]->As<ColumnInt64>()->Append(1);
        block[col++]->As<ColumnInt32>()->Append(1);
        block[col++]->As<ColumnFloat64>()->Append(10.56);
        block[col++]->As<ColumnFloat64>()->Append(100);
        block[col++]->As<ColumnFloat64>()->Append(234.67);
        block[col++]->As<ColumnString>()->Append("HTSC1111");
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

    return block;
}

int main(int argc, char* argv[]) {
    if (argc != 4) {
        std::cerr << "Usage: " << argv[0] << " <port> <total_events> <batch_size>" << std::endl;
        return 1;
    }

    const auto port         = std::stoi(argv[1]);
    const auto total_events = std::stoul(argv[2]);
    const auto batch_size   = std::stoul(argv[3]);

    Client client(ClientOptions().SetHost("localhost").SetPort(port));

    createTable(client);

    auto block = createBlock(batch_size);

    double max_time   = std::numeric_limits<double>::min();
    double min_time   = std::numeric_limits<double>::max();
    double total_time = 0;

    constexpr int num_iteration = 10;

    for (int iteration = 0; iteration < num_iteration; ++iteration) {
        auto start = std::chrono::high_resolution_clock::now();

        for (size_t i = 0; i < total_events / batch_size; ++i) {
            client.Insert("mds_sh_transaction", block);
        }

        auto end = std::chrono::high_resolution_clock::now();

        std::chrono::duration<double> elapsed = end - start;
        max_time                              = std::max(max_time, elapsed.count());
        min_time                              = std::min(min_time, elapsed.count());

        std::cout << "Iteration " << iteration + 1 << " : " << elapsed.count() << " sec\t" << total_events / elapsed.count() << " eps"
                  << std::endl;
        total_time += elapsed.count();
    }

    std::cout << std::endl;
    std::cout << "Total events: " << total_events << std::endl;
    std::cout << "Batch size: " << batch_size << std::endl;
    std::cout << "Throughtput: min = " << total_events / max_time << " max = " << total_events / min_time
              << " avg = " << total_events * num_iteration / total_time << std::endl;

    return 0;
}
