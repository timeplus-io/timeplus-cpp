C++ client for [Timeplus Enterprise](https://www.timeplus.com/) and [Timeplus Proton](https://github.com/timeplus-io/proton). The code is based on [clickhouse-cpp](https://github.com/ClickHouse/clickhouse-cpp/), with data type changes and other enhancements.

You can run DDL, streaming SQL, or data ingestion with this C++ client.

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


```
timeplus-cpp/examples/main.cpp
``` 

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
