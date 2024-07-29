
#include <timeplus/client.h>
#include <timeplus/error_codes.h>
#include <timeplus/types/type_parser.h>
#include <timeplus/base/socket.h>

#include <ut/utils.h>

#include <stdexcept>
#include <iostream>
#include <cmath>
#include <thread>
#include <iostream>
#if defined(_MSC_VER)
#   pragma warning(disable : 4996)
#endif

using namespace timeplus;
using namespace std;

inline void PrintBlock(const Block& block) {
    for (Block::Iterator bi(block); bi.IsValid(); bi.Next()) {
        std::cout << bi.Name() << " ";
    }
    std::cout << std::endl << block;
}

std::shared_ptr<ColumnArray> buildTestColumn(const std::vector<std::vector<std::string>>& rows) {
    auto arrayColumn = std::make_shared<ColumnArray>(std::make_shared<ColumnLowCardinalityT<ColumnString>>());

    for (const auto& row : rows) {
        auto column = std::make_shared<ColumnLowCardinalityT<ColumnString>>();

        for (const auto& string : row) {
            column->Append(string);
        }

        arrayColumn->AppendAsColumn(column);
    }

    return arrayColumn;
}

// INT256 边界值
// const std::string INT256_MIN = "-57896044618658097711785492504343953926634992332820282019728792003956564819968";
// const std::string INT256_MAX = "57896044618658097711785492504343953926634992332820282019728792003956564819967";
// const std::string UINT256_MAX = "115792089237316195423570985008687907853269984665640564039457584007913129639935";
Int256 max_val_256 = std::numeric_limits<Int256>::max();
Int256 min_val_256 = std::numeric_limits<Int256>::min();
UInt256 max_val_u256 = std::numeric_limits<UInt256>::max();

// INT128 边界值
// const std::string INT128_MIN = "-170141183460469231731687303715884105728";
// const std::string INT128_MAX = "170141183460469231731687303715884105727";
// const std::string UINT128_MAX = "340282366920938463463374607431768211455";

Int128 max_val_128 = std::numeric_limits<Int128>::max();
Int128 min_val_128 = std::numeric_limits<Int128>::min();
UInt256 max_val_u128 = std::numeric_limits<UInt128>::max();


void testIntType(Client& client) {

    // Create a table for each thread if necessary (optional, depends on the use case)
    client.Execute("DROP STREAM IF EXISTS test_insert_int");
    client.Execute("CREATE STREAM IF NOT EXISTS test_insert_int (id uint64, int8_val int8, int16_val int16, int32_val int32, int64_val int64, int128_val int128, int256_val int256, uint128_val uint128, uint256_val uint256)");

    Block block;

    auto id = std::make_shared<ColumnUInt64>();
    auto int8_col = std::make_shared<ColumnInt8>();
    auto int16_col = std::make_shared<ColumnInt16>();
    auto int32_col = std::make_shared<ColumnInt32>();
    auto int64_col = std::make_shared<ColumnInt64>();
    auto int128_col = std::make_shared<ColumnInt128>();
    auto int256_col = std::make_shared<ColumnInt256>();
    auto uint128_col = std::make_shared<ColumnUInt128>();
    auto uint256_col = std::make_shared<ColumnUInt256>();

    // Int8 boundaries
    int8_col->Append(INT8_MIN);
    int8_col->Append(0);
    int8_col->Append(INT8_MAX);

    // Int16 boundaries
    int16_col->Append(INT16_MIN);
    int16_col->Append(0);
    int16_col->Append(INT16_MAX);

    // Int8 boundaries
    int32_col->Append(INT32_MIN);
    int32_col->Append(0);
    int32_col->Append(INT32_MAX);

    // Int16 boundaries
    int64_col->Append(INT64_MIN);
    int64_col->Append(0);
    int64_col->Append(INT64_MAX);

    // Int128 boundaries
    int128_col->Append(min_val_128);
    int128_col->Append(static_cast<Int128>(0));
    int128_col->Append(max_val_128);

    // Int256 boundaries
    int256_col->Append(min_val_256);
    int256_col->Append(static_cast<Int256>(0));
    int256_col->Append(max_val_256);

    // UInt128 boundaries
    uint128_col->Append(static_cast<UInt128>(0));
    uint128_col->Append(max_val_u128/2);
    uint128_col->Append(max_val_u128);

    // UInt256 boundaries
    uint256_col->Append(static_cast<UInt256>(0));
    uint256_col->Append(max_val_u256/2);
    uint256_col->Append(max_val_u256);


    // Append IDs
    id->Append(1);
    id->Append(2);
    id->Append(3);

    block.AppendColumn("id", id);
    block.AppendColumn("int8_val", int8_col);
    block.AppendColumn("int16_val", int16_col);
    block.AppendColumn("int32_val", int32_col);
    block.AppendColumn("int64_val", int64_col);
    block.AppendColumn("int128_val", int128_col);
    block.AppendColumn("int256_val", int256_col);
    block.AppendColumn("uint128_val", uint128_col);
    block.AppendColumn("uint256_val", uint256_col);

    client.Insert("test_insert_int", block);

}


inline void testDateTimeType(Client& client) {
    Block b;

    /// Delete table.
    client.Execute("DROP STREAM IF EXISTS test_datetime");
    /// Create a table.
    client.Execute("CREATE STREAM IF NOT EXISTS test_datetime (d date, d32 date32, dt32 datetime, dt64 datetime64(3))");
    
    auto d = std::make_shared<ColumnDate>();
    auto d32 = std::make_shared<ColumnDate32>();
    auto dt32 = std::make_shared<ColumnDateTime>();
    auto dt64 = std::make_shared<ColumnDateTime64>(3);

    d->Append(std::time(nullptr));
    d32->Append(std::time(nullptr));
    dt32->Append(std::time(nullptr));
    dt64->Append(std::time(nullptr) * 1000000 + 123456);

    d->Append(0);
    d32->Append(0);
    dt32->Append(0);
    dt64->Append(0);

    d->Append(100);
    d32->Append(4294967295);
    dt32->Append(4294967295);
    dt64->Append(9223372036854775807);

    b.AppendColumn("d", d);
    b.AppendColumn("d32", d32);
    b.AppendColumn("dt32", dt32);
    b.AppendColumn("dt64", dt64);

    client.Insert("test_datetime", b);

    // client.Select("SELECT dt32, dt64 FROM test_datetime", [](const Block& block)
    //     {
    //         for (size_t c = 0; c < block.GetRowCount(); ++c) {
    //             auto print_value = [&](const auto& col) {
    //                 std::time_t t = col->At(c);
    //                 std::cerr << std::asctime(std::localtime(&t));
    //                 std::cerr << col->Timezone() << std::endl;
    //             };

    //             print_value(block[0]->As<ColumnDateTime>());
    //             print_value(block[1]->As<ColumnDateTime64>());

    //         }
    //     }
    // );

    /// Delete table.
    // client.Execute("DROP STREAM test_datetime");
}

inline void testDecimalType(Client& client) {
    Block b;

    client.Execute("DROP STREAM IF EXISTS test_cpp_decimal");
    /// Create a table.
    client.Execute(
        "CREATE STREAM IF NOT EXISTS "
        "test_cpp_decimal (id uint64, d1 decimal(9, 4), d2 decimal(18, 9), d3 decimal(38, 19), "
        "                         d4 decimal32(4), d5 decimal64(9), d6 decimal128(19), d7 decimal(76, 38), d8 decimal256(38))");


    auto id = std::make_shared<ColumnUInt64>();
    auto d1 = std::make_shared<ColumnDecimal>(9, 4);
    auto d2 = std::make_shared<ColumnDecimal>(18, 9);
    auto d3 = std::make_shared<ColumnDecimal>(38, 19);
    auto d4 = std::make_shared<ColumnDecimal>(9, 4);
    auto d5 = std::make_shared<ColumnDecimal>(18, 9);
    auto d6 = std::make_shared<ColumnDecimal>(38, 19);
    auto d7 = std::make_shared<ColumnDecimal>(76, 38);
    auto d8 = std::make_shared<ColumnDecimal>(76, 38);


    id->Append(1);
    d1->Append(123456789);
    d2->Append(123456789012345678);
    d3->Append(1234567890123456789);
    d4->Append(123456789);
    d5->Append(123456789012345678);
    d6->Append(1234567890123456789);
    d7->Append(1234567890123456789);
    d8->Append(1234567890123456789);

    id->Append(2);
    d1->Append(999999999);
    d2->Append(999999999999999999);
    d3->Append(999999999999999999);
    d4->Append(999999999);
    d5->Append(999999999999999999);
    d6->Append(999999999999999999);
    d7->Append(999999999999999999);
    d8->Append(999999999999999999);

    id->Append(3);
    d1->Append(-999999999);
    d2->Append(-999999999999999999);
    d3->Append(-999999999999999999);
    d4->Append(-999999999);
    d5->Append(-999999999999999999);
    d6->Append(-999999999999999999);
    d7->Append(-999999999999999999);
    d8->Append(-999999999999999999);

    // Check strings with decimal point
    id->Append(4);
    d1->Append(static_cast<std::string>("12345.6789"));
    d2->Append(static_cast<std::string>("123456789.012345678"));
    d3->Append(static_cast<std::string>("1234567890123456789.0123456789012345678"));
    d4->Append(static_cast<std::string>("12345.6789"));
    d5->Append(static_cast<std::string>("123456789.012345678"));
    d6->Append(static_cast<std::string>("1234567890123456789.0123456789012345678"));
    d7->Append(static_cast<std::string>("12345678901234567890123456789012345678.90123456789012345678901234567890123456"));
    d8->Append(static_cast<std::string>("12345678901234567890123456789012345678.90123456789012345678901234567890123456"));

    id->Append(5);
    d1->Append(static_cast<std::string>("-12345.6789"));
    d2->Append(static_cast<std::string>("-123456789.012345678"));
    d3->Append(static_cast<std::string>("-1234567890123456789.0123456789012345678"));
    d4->Append(static_cast<std::string>("-12345.6789"));
    d5->Append(static_cast<std::string>("-123456789.012345678"));
    d6->Append(static_cast<std::string>("-1234567890123456789.0123456789012345678"));
    d7->Append(static_cast<std::string>("-12345678901234567890123456789012345678.90123456789012345678901234567890123456"));
    d8->Append(static_cast<std::string>("-12345678901234567890123456789012345678.90123456789012345678901234567890123456"));

    // id->Append(6);
    // d1->Append(static_cast<std::string>("12345.678"));
    // d2->Append(static_cast<std::string>("123456789.0123456789"));
    // d3->Append(static_cast<std::string>("1234567890123456789.0123456789012345678"));
    // d4->Append(static_cast<std::string>("12345.6789"));
    // d5->Append(static_cast<std::string>("123456789.012345678"));
    // d6->Append(static_cast<std::string>("1234567890123456789.0123456789012345678"));

    b.AppendColumn("id", id);
    b.AppendColumn("d1", d1);
    b.AppendColumn("d2", d2);
    b.AppendColumn("d3", d3);
    b.AppendColumn("d4", d4);
    b.AppendColumn("d5", d5);
    b.AppendColumn("d6", d6);
    b.AppendColumn("d7", d7);
    b.AppendColumn("d8", d8);

    client.Insert("test_cpp_decimal", b);

    // client.Select("SELECT * FROM test_cpp_decimal", [](const Block& block)
    //     {
    //         for (size_t i = 0; i < block.GetRowCount(); ++i) {
    //             std::cout << (*block[0]->As<ColumnUInt64>())[i] << " "
    //                       << (*block[1]->As<ColumnDecimal>())(i) << "\n";
    //         }
    //     }
    // );


    /// Delete table.
    // client.Execute("DROP STREAM test_cpp_decimal");
}

inline void testEnumType(Client& client) {

    client.Execute("DROP STREAM IF EXISTS test_enums");
    /// Create a table.
    client.Execute("CREATE STREAM IF NOT EXISTS test_enums (id uint64, e1 enum8('Min' = -128, 'Max' = 127), e2 enum16('Mn' = -32768, 'Mx' = 32767))");

    /// Insert some values.
    {
        Block block;

        auto id = std::make_shared<ColumnUInt64>();
        id->Append(1);
        id->Append(2);

        auto e1 = std::make_shared<ColumnEnum8>(Type::CreateEnum8({{"Min", -128}, {"Max", 127}}));
        e1->Append(-128);
        e1->Append("MAX");

        auto e2 = std::make_shared<ColumnEnum16>(Type::CreateEnum16({{"Mn", -32768}, {"Mx", 32767}}));
        e2->Append("Mn");
        e2->Append("Mx");

        block.AppendColumn("id", id);
        block.AppendColumn("e1", e1);
        block.AppendColumn("e2", e2);

        client.Insert("test_enums", block);
    }

    /// Select values inserted in the previous step.
    // client.Select("SELECT id, e1, e2 FROM test_enums", [](const Block& block)
    //     {
    //         for (Block::Iterator bi(block); bi.IsValid(); bi.Next()) {
    //             std::cout << bi.Name() << " ";
    //         }
    //         std::cout << std::endl;

    //         for (size_t i = 0; i < block.GetRowCount(); ++i) {
    //             std::cout << (*block[0]->As<ColumnUInt64>())[i] << " "
    //                       << (*block[1]->As<ColumnEnum8>()).NameAt(i) << " "
    //                       << (*block[2]->As<ColumnEnum16>()).NameAt(i) << "\n";
    //         }
    //     }
    // );

    /// Delete table.
    // client.Execute("DROP STREAM test_enums");
}

inline void testFloatType(Client& client) {
    /// Create a table.
    client.Execute("DROP STREAM IF EXISTS test_float");
    client.Execute("CREATE STREAM IF NOT EXISTS test_float (f32 float32, f64 float64)");

    /// Insert some values.
    {
        Block block;

        // float32 的最大值和最小值
        float max_float32 = std::numeric_limits<float>::max();
        float min_float32 = std::numeric_limits<float>::min();
        
        // float64 的最大值和最小值
        double max_float64 = std::numeric_limits<double>::max();
        double min_float64 = std::numeric_limits<double>::min();

        auto f32 = std::make_shared<ColumnFloat32>();
        f32->Append(min_float32);
        f32->Append(max_float32);

        auto f64 = std::make_shared<ColumnFloat64>();
        f64->Append(min_float64);
        f64->Append(max_float64);

        block.AppendColumn("f32", f32);
        block.AppendColumn("f64", f64);

        client.Insert("test_float", block);
    }

    /// Select values inserted in the previous step.
    // client.Select("SELECT f32, f64 FROM test_float", [](const Block& block)
    //     {
    //         // for (Block::Iterator bi(block); bi.IsValid(); bi.Next()) {
    //         //     std::cout << bi.Name() << " ";
    //         // }
    //         // std::cout << std::endl;

    //         // for (size_t i = 0; i < block.GetRowCount(); ++i) {
    //         //     std::cout << (*block[0]->As<ColumnFloat32>())[i] << " "
    //         //               << (*block[1]->As<ColumnFloat64>())(i) << "\n";
    //         // }
    //         std::cout << PrettyPrintBlock{block} << std::endl;
    //     }
    // );

    /// Delete table.
    // client.Execute("DROP STREAM test_float");
}

inline void testIPType(Client & client) {
    /// Create a table.
    client.Execute("DROP STREAM IF EXISTS test_ips");
    client.Execute("CREATE STREAM IF NOT EXISTS test_ips (id uint64, v4 ipv4, v6 ipv6)");

    /// Insert some values.
    {
        Block block;

        auto id = std::make_shared<ColumnUInt64>();
        id->Append(1);
        id->Append(2);
        id->Append(3);

        auto v4 = std::make_shared<ColumnIPv4>();
        v4->Append("127.0.0.1");
        v4->Append(3585395774);
        v4->Append(0);

        auto v6 = std::make_shared<ColumnIPv6>();
        v6->Append("::1");
        v6->Append("aa::ff");
        v6->Append("fe80::86ba:ef31:f2d8:7e8b");

        block.AppendColumn("id", id);
        block.AppendColumn("v4", v4);
        block.AppendColumn("v6", v6);

        client.Insert("test_ips", block);
    }

    /// Select values inserted in the previous step.
    // client.Select("SELECT id, v4, v6 FROM test_ips", [&](const Block& block)
    //     {

    //         // for (Block::Iterator bi(block); bi.IsValid(); bi.Next()) {
    //         //     std::cout << bi.Name() << " ";
    //         // }
    //         // std::cout << std::endl;

    //         for (size_t i = 0; i < block.GetRowCount(); ++i) {
    //             std::cout << (*block[0]->As<ColumnUInt64>())[i] << " "
    //                       << (*block[1]->As<ColumnIPv4>()).AsString(i) << " (" << (*block[1]->As<ColumnIPv4>())[i].s_addr << ") "
    //                       << (*block[2]->As<ColumnIPv6>()).AsString(i) << "\n";
    //         }

    //     }
    // );

    /// Delete table.
    // client.Execute("DROP STREAM test_ips");
}

// inline void testStringType(Client &client){

// }

inline void testLowCardinalityType(Client& client) {
    /// test tuple
    Block block;
    const auto testData = std::vector<std::vector<std::string>> {
        { "aa", "bb" },
        { "cc"},
        { "dd" },
        { "aa", "ee"}
    };  

    auto column = buildTestColumn(testData);

    block.AppendColumn("arr", column);

    client.Execute("DROP TEMPORARY STREAM IF EXISTS array_lc");
    client.Execute("CREATE TEMPORARY STREAM IF NOT EXISTS array_lc (arr array(low_cardinality(string))) ENGINE = Memory");
    client.Insert("array_lc", block);

    client.Select("SELECT * FROM array_lc", [&](const Block& bl) {
    for (size_t c = 0; c < bl.GetRowCount(); ++c) {
        auto col = bl[0]->As<ColumnArray>()->GetAsColumn(c);
        for (size_t i = 0; i < col->Size(); ++i) {
            if (auto string_column = col->As<ColumnString>()) {
                const auto ts = string_column->At(i);
                std::cout<< ts <<std::endl;
            } else if (auto lc_string_column = col->As<ColumnLowCardinalityT<ColumnString>>()) {
                const auto ts = lc_string_column->At(i);
                std::cout<< ts <<std::endl;
            } 
            
        }
    }
    });
}


inline void testArrayType(Client& client) {
    Block b;

    client.Execute("DROP STREAM IF EXISTS test_array_types");
    client.Execute("CREATE STREAM IF NOT EXISTS test_array_types "
                   "(arr_int128 array(int128), arr_uint128 array(uint128), "
                   "arr_decimal array(decimal(10,2)), arr_float32 array(float32), arr_float64 array(float64), "
                   "arr_ip array(ipv4), arr_enum8 array(enum8('First' = 1, 'Second' = 2)))");

    auto arr_int128 = std::make_shared<ColumnArray>(std::make_shared<ColumnInt128>());
    auto arr_uint128 = std::make_shared<ColumnArray>(std::make_shared<ColumnUInt128>());
    auto arr_decimal = std::make_shared<ColumnArray>(std::make_shared<ColumnDecimal>(10,2));
    auto arr_float32 = std::make_shared<ColumnArray>(std::make_shared<ColumnFloat32>());
    auto arr_float64 = std::make_shared<ColumnArray>(std::make_shared<ColumnFloat64>());
    auto arr_ip = std::make_shared<ColumnArray>(std::make_shared<ColumnIPv4>());
    auto arr_enum8 = std::make_shared<ColumnArray>(std::make_shared<ColumnEnum8>(Type::CreateEnum8({{"First", 1}, {"Second", 2}})));

    // Append data to arrays

    auto i128 = std::make_shared<ColumnInt128>();
    i128->Append(1);
    i128->Append(2);
    arr_int128->AppendAsColumn(i128);

    auto u128 = std::make_shared<ColumnUInt128>();
    u128->Append(1);
    u128->Append(2);
    arr_uint128->AppendAsColumn(u128);

    auto de = std::make_shared<ColumnDecimal>(10,2);
    de->Append(123.45);
    de->Append(678.90);
    arr_decimal->AppendAsColumn(de);

    auto f32 = std::make_shared<ColumnFloat32>();
    f32->Append(1.23f);
    f32->Append(4.56f);
    arr_float32->AppendAsColumn(f32);

    auto f64 = std::make_shared<ColumnFloat64>();
    f64->Append(1.23);
    f64->Append(4.56);
    arr_float64->AppendAsColumn(f64);

    auto ip = std::make_shared<ColumnIPv4>();
    ip->Append("127.0.0.1");
    ip->Append("192.168.1.1");
    arr_ip->AppendAsColumn(ip);

    auto e8 = std::make_shared<ColumnEnum8>(Type::CreateEnum8({{"First", 1}, {"Second", 2}}));
    e8->Append(1);
    e8->Append(2);
    arr_enum8->AppendAsColumn(e8);


    b.AppendColumn("arr_int128", arr_int128);
    b.AppendColumn("arr_uint128", arr_uint128);
    b.AppendColumn("arr_decimal", arr_decimal);
    b.AppendColumn("arr_float32", arr_float32);
    b.AppendColumn("arr_float64", arr_float64);
    b.AppendColumn("arr_ip", arr_ip);
    b.AppendColumn("arr_enum8", arr_enum8);

    client.Insert("test_array_types", b);

    // client.Select("SELECT arr FROM test_array", [](const Block& block)
    //     {
    //         for (size_t c = 0; c < block.GetRowCount(); ++c) {
    //             auto col = block[0]->As<ColumnArray>()->GetAsColumn(c);
    //             for (size_t i = 0; i < col->Size(); ++i) {
    //                 std::cerr << (int)(*col->As<ColumnUInt64>())[i] << " ";
    //             }
    //             std::cerr << std::endl;
    //         }
    //     }
    // );

    /// Delete table.
    // client.Execute("DROP STREAM test_array");
}

inline void testMultitesArrayType(Client& client) {
    Block b;

    /// Create a table.
    client.Execute("CREATE STREAM IF NOT EXISTS test_multiarray (arr array(array(uint64)))");

    auto arr = std::make_shared<ColumnArray>(std::make_shared<ColumnUInt64>());

    auto id = std::make_shared<ColumnUInt64>();
    id->Append(17);
    arr->AppendAsColumn(id);

    auto a2 = std::make_shared<ColumnArray>(std::make_shared<ColumnArray>(std::make_shared<ColumnUInt64>()));
    a2->AppendAsColumn(arr);
    b.AppendColumn("arr", a2);
    client.Insert("test_multiarray", b);

    client.Select("SELECT arr FROM test_multiarray", [](const Block& block)
        {
            for (size_t c = 0; c < block.GetRowCount(); ++c) {
                auto col = block[0]->As<ColumnArray>()->GetAsColumn(c);
                cout << "[";
                for (size_t i = 0; i < col->Size(); ++i) {
                    auto col2 = col->As<ColumnArray>()->GetAsColumn(i);
                    for (size_t j = 0; j < col2->Size(); ++j) {
                        cout << (int)(*col2->As<ColumnUInt64>())[j];
                        if (j + 1 != col2->Size()) {
                            cout << " ";
                        }
                    }
                }
                std::cout << "]" << std::endl;
            }
        }
    );

    /// Delete table.
    client.Execute("DROP TREAM test_multiarray");
}

inline void testTupletType(Client& client) {
    /// test tuple
    Block block;
    auto tupls = std::make_shared<ColumnTuple>(std::vector<ColumnRef>{
        std::make_shared<ColumnUInt64>(),
        std::make_shared<ColumnString>()});


    auto val = tupls->CloneEmpty()->As<ColumnTuple>();

    (*val)[0]->AsStrict<ColumnUInt64>()->Append(1);
    (*val)[1]->AsStrict<ColumnString>()->Append("123");

    (*val)[0]->AsStrict<ColumnUInt64>()->Append(2);
    (*val)[1]->AsStrict<ColumnString>()->Append("def");


    block.AppendColumn("tup", tupls);

    client.Execute("DROP STREAM IF EXISTS test_tuple");
    client.Execute("CREATE STREAM IF NOT EXISTS test_tuple (tup tuple(uint64, string))");
    client.Insert("test_tuple", block);

    // client.Select("SELECT * FROM test_tuple", [&](const Block& bl){
    //     // std::cout<<bl.GetColumnName(0)<<std::endl;
    // });
}

inline void testMapType(Client& client) {
    /// test tuple
    Block block;
    auto m1 = std::make_shared<ColumnMapT<ColumnUInt256, ColumnString>>(
    std::make_shared<ColumnUInt256>(),
    std::make_shared<ColumnString>());

    // auto m2 = std::make_shared<ColumnMapT<ColumnInt256, ColumnDecimal>>(
    // std::make_shared<ColumnInt256>(),
    // std::make_shared<ColumnDecimal>(76,38));

    std::map<UInt256, std::string> val1;
    val1[0] = "123";
    val1[1] = "abc";


    (*m1).Append(val1);

    block.AppendColumn("m1",m1);
    
    client.Execute("DROP STREAM IF EXISTS test_map");
    client.Execute("CREATE STREAM IF NOT EXISTS test_map (m1 map(uint256, string))");
    client.Insert("test_map", block);

    // client.Select("SELECT * FROM test_map", [&](const Block& bl){
    //     // std::cout<<bl.GetColumnName(0)<<std::endl;
    // });
}

// inline void testNestedType(Client& client) {

// }

inline void GenericExample(Client& client) {
    /// Create a table.
    client.Execute("CREATE STREAM IF NOT EXISTS test_client (id uint64, name string)");

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

        client.Insert("test_client", block);
    }

    /// Select values inserted in the previous step.
    client.Select("SELECT id, name FROM test_client", [](const Block& block)
        {
            std::cout << PrettyPrintBlock{block} << std::endl;
        }
    );

    /// Delete table.
    client.Execute("DROP STREAM test_client");
}

inline void NullableExample(Client& client) {
    /// Create a table.
    client.Execute("CREATE STREAM IF NOT EXISTS test_client (id nullable(uint64), date nullable(date))");

    /// Insert some values.
    {
        Block block;

        {
            auto id = std::make_shared<ColumnUInt64>();
            id->Append(1);
            id->Append(2);

            auto nulls = std::make_shared<ColumnUInt8>();
            nulls->Append(0);
            nulls->Append(0);

            block.AppendColumn("id", std::make_shared<ColumnNullable>(id, nulls));
        }

        {
            auto date = std::make_shared<ColumnDate>();
            date->Append(std::time(nullptr));
            date->Append(std::time(nullptr));

            auto nulls = std::make_shared<ColumnUInt8>();
            nulls->Append(0);
            nulls->Append(1);

            block.AppendColumn("date", std::make_shared<ColumnNullable>(date, nulls));
        }

        client.Insert("test_client", block);
    }

    /// Select values inserted in the previous step.
    client.Select("SELECT id, date FROM test_client", [](const Block& block)
        {
            for (size_t c = 0; c < block.GetRowCount(); ++c) {
                auto col_id   = block[0]->As<ColumnNullable>();
                auto col_date = block[1]->As<ColumnNullable>();

                if (col_id->IsNull(c)) {
                    std::cerr << "\\N ";
                } else {
                    std::cerr << col_id->Nested()->As<ColumnUInt64>()->At(c)
                              << " ";
                }

                if (col_date->IsNull(c)) {
                    std::cerr << "\\N\n";
                } else {
                    std::time_t t = col_date->Nested()->As<ColumnDate>()->At(c);
                    std::cerr << std::asctime(std::localtime(&t))
                              << "\n";
                }
            }
        }
    );

    /// Delete table.
    client.Execute("DROP STREAM test_client");
}

inline void NumbersExample(Client& client) {
    size_t num = 0;

    client.Select("SELECT number, number FROM system.numbers LIMIT 100000", [&num](const Block& block)
        {
            if (Block::Iterator(block).IsValid()) {
                auto col = block[0]->As<ColumnUInt64>();

                for (size_t i = 0; i < col->Size(); ++i) {
                    if (col->At(i) < num) {
                        throw std::runtime_error("invalid sequence of numbers");
                    }

                    num = col->At(i);
                }
            }
        }
    );
}

inline void CancelableExample(Client& client) {
    /// Create a table.
    client.Execute("CREATE STREAM IF NOT EXISTS test_client (x uint64)");

    /// Insert a few blocks.
    for (unsigned j = 0; j < 10; j++) {
        Block b;

        auto x = std::make_shared<ColumnUInt64>();
        for (uint64_t i = 0; i < 1000; i++) {
            x->Append(i);
        }

        b.AppendColumn("x", x);
        client.Insert("test_client", b);
    }

    /// Send a query which is canceled after receiving the first block (note:
    /// due to the low number of rows in this test, this will not actually have
    /// any effect, it just tests for errors)
    client.SelectCancelable("SELECT * FROM test_client", [](const Block&)
        {
            return false;
        }
    );

    /// Delete table.
    client.Execute("DROP STREAM test_client");
}

inline void ExecptionExample(Client& client) {
    /// Create a table.
    client.Execute("CREATE STREAM IF NOT EXISTS test_exceptions (id uint64, name string)");
    /// Expect failing on table creation.
    try {
        client.Execute("CREATE STREAM test_exceptions (id uint64, name string)");
    } catch (const ServerException& e) {
        if (e.GetCode() == ErrorCodes::TABLE_ALREADY_EXISTS) {
            // OK
        } else {
            throw;
        }
    }

    /// Delete table.
    client.Execute("DROP STREAM test_exceptions");
}

inline void SelectNull(Client& client) {
    client.Select("SELECT NULL", []([[maybe_unused]] const Block& block)
        {
            assert(block.GetRowCount() < 2);
            (void)(block);
        }
    );
}

inline void ShowTables(Client& client) {
    /// Select values inserted in the previous step.
    client.Select("SHOW STREAMS", [](const Block& block)
        {
            for (size_t i = 0; i < block.GetRowCount(); ++i) {
                std::cout << (*block[0]->As<ColumnString>())[i] << "\n";
            }
        }
    );
}


static void RunTests(Client& client) {
    // testIntType(client); //1
    // testDecimalType(client); //1
    testArrayType(client); //
    // CancelableExample(client);
    // testDateTimeType(client); //1
    // testFloatType(client); //1
    // CancelableExample(client);
    // testEnumType(client); //2
    // ExecptionExample(client);
    // GenericExample(client);
    // testIPType(client); //1
    // MultitestArrayType(client);
    // NullableExample(client);
    // NumbersExample(client);
    // SelectNull(client);
    // ShowTables(client);
}


int main()
{
    /// Initialize client connection.
    try {
        const auto localHostEndpoint = ClientOptions()
                .SetHost("localhost")
                .SetPort(8463);

        {
            Client client(ClientOptions(localHostEndpoint)
                    .SetPingBeforeQuery(true));
            RunTests(client);
            // std::cout << "current endpoint : " <<  client.GetCurrentEndpoint().value().host << "\n";
        }

    } catch (const std::exception& e) {
        std::cerr << "exception : " << e.what() << std::endl;
    }


    return 0;
}

// void createAndSelect(Client& client) {

//     /// Create a table.
//     client.Execute("CREATE STREAM IF NOT EXISTS default.numbers (id uint64, name string)");

//     /// Select values inserted in the previous step.
//     client.Select("SELECT id, name FROM default.numbers", [] (const Block& block)
//         {
//             for (size_t i = 0; i < block.GetRowCount(); ++i) {
//                 std::cout << block[0]->As<ColumnUInt64>()->At(i) << " "
//                           << block[1]->As<ColumnString>()->At(i) << "\n";
//             }
//         }
//     );

// }

// void insertStream(Client& client) {

//     /// Insert some values.
//     {
//         Block block;

//         auto id = std::make_shared<ColumnUInt64>();
//         id->Append(1);
//         id->Append(7);

//         auto name = std::make_shared<ColumnString>();
//         name->Append("one");
//         name->Append("seven");

//         block.AppendColumn("id"  , id);
//         block.AppendColumn("name", name);

//         client.Insert("default.numbers", block);
//     }
// }

// void dropStream(Client& client) {

//     /// Delete stream.
//     client.Execute("DROP STREAM IF EXISTS default.numbers");
// }

// void (*functionPointers[])(Client&) = {createAndSelect, insertStream, dropStream};

// int main(int argc, char* argv[]) {
//     if (argc != 2) {
//         std::cerr << "Usage: " << argv[0] << " <function_number>" << std::endl;
//         return 1;
//     }

//     int functionNumber = std::stoi(argv[1]);

//     if (functionNumber < 1 || functionNumber > 3) {
//         std::cerr << "Invalid function number. Please enter: "<<"\n"
//                   << "1 (create stream)" <<"\n"
//                   << "2 (insert into stream)" <<"\n"
//                   << "3 (delete stream)" <<std::endl;
//         return 1;
//     }

//     /// Initialize client connection.
//     Client client(ClientOptions().SetHost("localhost").SetPort(8463));

//     functionPointers[functionNumber - 1](client);

//     return 0;
// }
