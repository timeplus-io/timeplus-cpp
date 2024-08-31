C++ client for [Timeplus Enterprise](https://www.timeplus.com/) and [Timeplus Proton](https://github.com/timeplus-io/proton). The code is based on [clickhouse-cpp](https://github.com/ClickHouse/clickhouse-cpp/), with data type changes and other enhancements.

You can run DDL, streaming queries, or data ingestion with this C++ client. Both sync and async inserts are supported, with idempotent_id support.

## Supported data types

* array(T)
* date
* datetime, datetime64
* datetime([timezone]), datetime64(N, [timezone])
* decimal32, decimal64, decimal128, decimal256
* enum8, enum16
* fixed_string(N)
* float32, float64
* ipv4, ipv6
* nullable(T)
* string
* low_cardinality(string) or low_cardinality(fixed_string(N))
* tuple
* uint8, uint16, uint32, uint64, uint128, uint256, int8, int16, int32, int64, int128, int256
* uuid
* map

## Dependencies
In the most basic case one needs only:
- a C++-17-complaint compiler,
- `cmake` (3.12 or newer), and
- `ninja`

Optional dependencies:
- openssl
- liblz4
- libabsl
- libzstd


## Building

```sh
$ mkdir build .
$ cd build
$ cmake -D CMAKE_BUILD_TYPE=Release -D BUILD_TESTS=ON -D BUILD_EXAMPLES=ON -D BUILD_GTEST=ON ..
$ make
```


## Example application build with timeplus-cpp

### sample code
Please check the examples in https://github.com/timeplus-io/timeplus-cpp/tree/master/examples folder.

```cpp
#include <iostream>
#include <timeplus/client.h>

using namespace timeplus;

void createAndSelect(Client& client) {
    /// Initialize client connection.
    Client client(ClientOptions().SetHost("localhost").SetPort(8463));// your server's port

    /// Create a table.
    client.Execute("CREATE STREAM IF NOT EXISTS default.numbers (id uint64, name string)");

    /// Select values inserted in the previous step.
    client.Select("SELECT id, name FROM default.numbers", [] (const Block& block)
        {
            for (size_t i = 0; i < block.GetRowCount(); ++i) {
                std::cout << block[0]->As<ColumnUInt64>()->At(i) << " "
                          << block[1]->As<ColumnString>()->At(i) << "\n";
            }
        }
    );

}

void insertStream(Client& client) {

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
}

void dropStream(Client& client) {
    /// Delete stream.
    client.Execute("DROP STREAM  IF EXISTS default.numbers");
}

void (*functionPointers[])(Client&) = {createAndSelect, insertStream};

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
```



```sh
# terminal 1
# current path should be /timeplus-cpp
# create stream and start select
$ ./build/example/timeplus-client 1

# new terminal 2
# insert into stream
$ ./build/example/timeplus-client 2

# back to terminal 1, the insert values will be printed

# terminal 2
# delete stream
$ ./build/example/timeplus-client 3
```
