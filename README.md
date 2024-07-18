Timeplus C++ client 

C++ client for [Timeplus](https://www.timeplus.com/).

## Supported data types

* Array(T)
* Date
* DateTime, DateTime64
* DateTime([timezone]), DateTime64(N, [timezone])
* Decimal32, Decimal64, Decimal128
* Enum8, Enum16
* FixedString(N)
* Float32, Float64
* IPv4, IPv6
* Nullable(T)
* String
* LowCardinality(String) or LowCardinality(FixedString(N))
* Tuple
* UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64
* Int128
* UUID
* Map
* Point, Ring, Polygon, MultiPolygon

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
$ cmake .. [-DBUILD_TESTS=ON]
$ make
```

Plese refer to the workflows for the reference on dependencies/build options
- https://github.com/timeplus-io/timeplus-cpp/blob/master/.github/workflows/linux.yml
- https://github.com/timeplus-io/timeplus-cpp/blob/master/.github/workflows/windows_msvc.yml
- https://github.com/timeplus-io/timeplus-cpp/blob/master/.github/workflows/windows_mingw.yml
- https://github.com/timeplus-io/timeplus-cpp/blob/master/.github/workflows/macos.yml


## Example application build with timeplus-cpp

There are various ways to integrate clickhouse-cpp with the build system of an application. Below example uses the simple approach based on
submodules presented in https://www.youtube.com/watch?v=ED-WUk440qc .

- `mkdir timeplus-cpp && cd timeplus-cpp && git init`
- `git submodule add https://github.com/timeplus-io/timeplus-cpp.git contribs/timeplus-cpp`
- `touch app.cpp`, then copy the following C++ code into that file

```cpp
#include <iostream>
#include <timeplus/client.h>

using namespace timeplus;

int main()
{
    /// Initialize client connection.
    Client client(ClientOptions().SetHost("localhost"));

    /// Create a table.
    client.Execute("CREATE TABLE IF NOT EXISTS default.numbers (id UInt64, name String) ENGINE = Memory");

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

    /// Select values inserted in the previous step.
    client.Select("SELECT id, name FROM default.numbers", [] (const Block& block)
        {
            for (size_t i = 0; i < block.GetRowCount(); ++i) {
                std::cout << block[0]->As<ColumnUInt64>()->At(i) << " "
                          << block[1]->As<ColumnString>()->At(i) << "\n";
            }
        }
    );

    /// Delete table.
    client.Execute("DROP TABLE default.numbers");

    return 0;
}
```

- `touch CMakeLists.txt`, then copy the following CMake code into that file

```cmake
cmake_minimum_required(VERSION 3.12)
project(application-example)

set(CMAKE_CXX_STANDARD 17)

add_subdirectory(contribs/timeplus-cpp)

add_executable(${PROJECT_NAME} "app.cpp")

target_include_directories(${PROJECT_NAME} PRIVATE contribs/timeplus-cpp/ contribs/timeplus-cpp/contrib/absl)

target_link_libraries(${PROJECT_NAME} PRIVATE timeplus-cpp-lib)
```

- run `rm -rf build && cmake -B build -S . && cmake --build build -j32` to remove remainders of the previous builds, run CMake and build the
  application. The generated binary is located in location `build/application-example`.

## Thread-safety
⚠ Please note that `Client` instance is NOT thread-safe. I.e. you must create a separate `Client` for each thread or utilize some synchronization techniques. ⚠

## Retries
If you wish to implement some retry logic atop of `timeplus::Client` there are few simple rules to make you life easier:
- If previous attempt threw an exception, then make sure to call `timeplus::Client::ResetConnection()` before the next try.
- For `timeplus::Client::Insert()` you can reuse a block from previous try, no need to rebuild it from scratch.

See https://github.com/ClickHouse/clickhouse-cpp/issues/184 for details.
