#include <clickhouse/client.h>
#include <clickhouse/error_codes.h>
#include <clickhouse/types/type_parser.h>
#include <clickhouse/base/socket.h>

#include <ut/utils.h>

#include <stdexcept>
#include <iostream>
#include <cmath>
#include <thread>
#include <iostream>
#if defined(_MSC_VER)
#   pragma warning(disable : 4996)
#endif

using namespace clickhouse;
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


inline void testIntType(Client& client) {
    Block b;

    /// Create a table.
    client.Execute(
        "CREATE TEMPORARY STREAM IF NOT EXISTS test_int (bo bool, i8 int8, i16 int16, i32 int32, i64 int64, i128 int128, i256 int256, ui8 uint8, ui16 uint16, ui32 uint32, ui64 uint64, ui128 uint128, ui256 uint256) ENGINE = Memory"
    );

    auto bo = std::make_shared<ColumnUInt8>();
    auto i8 = std::make_shared<ColumnInt8>();
    auto i16 = std::make_shared<ColumnInt16>();
    auto i32 = std::make_shared<ColumnInt32>();
    auto i64 = std::make_shared<ColumnInt64>();
    auto i128 = std::make_shared<ColumnInt128>();
    auto i256 = std::make_shared<ColumnInt256>();

    bo->Append(false);
    i8->Append(1);
    i16->Append(2);
    i32->Append(3);
    i64->Append(4);
    i128->Append(5);
    i256->Append(6);

    b.AppendColumn("bo", bo);
    b.AppendColumn("i8", i8);
    b.AppendColumn("i16", i16);
    b.AppendColumn("i32", i32);
    b.AppendColumn("i64", i64);
    b.AppendColumn("i128", i128);
    b.AppendColumn("i256", i256);

    auto ui8 = std::make_shared<ColumnUInt8>();
    auto ui16 = std::make_shared<ColumnUInt16>();
    auto ui32 = std::make_shared<ColumnUInt32>();
    auto ui64 = std::make_shared<ColumnUInt64>();
    auto ui128 = std::make_shared<ColumnUInt128>();
    auto ui256 = std::make_shared<ColumnUInt256>();
    ui8->Append(7);
    ui16->Append(8);
    ui32->Append(9);
    ui64->Append(10);
    ui128->Append(11);
    ui256->Append(12);


    b.AppendColumn("ui8", ui8);
    b.AppendColumn("ui16", ui16);
    b.AppendColumn("ui32", ui32);
    b.AppendColumn("ui64", ui64);
    b.AppendColumn("ui128", ui128);
    b.AppendColumn("ui256", ui256);


    client.Insert("test_int", b);

    // client.Select("SELECT * FROM test_int", [](const Block& block)
    //     {
    //         std::cout << PrettyPrintBlock{block} << std::endl;
    //     }
    // );

    client.Select("SELECT bo, i8, i16, i32, i64, i128, i256, ui8, ui16, ui32, ui64, ui128, ui256 FROM test_int", [](const Block& block)
        {
            // for (size_t i = 0; i < block.GetRowCount(); ++i) {
            //     std::cout << (*block[0]->As<ColumnUInt8>())[i] << " "
            //                 << (*block[1]->As<ColumnInt8>())[i] << " "
            //                 << (*block[2]->As<ColumnInt16>())[i] << " "
            //                 << (*block[3]->As<ColumnInt32>())[i] << " " 
            //                 << (*block[4]->As<ColumnInt64>())[i] << " "
            //                 << (*block[5]->As<ColumnInt128>())[i] << " "
            //                 << (*block[6]->As<ColumnInt256>())[i] << " "
            //                 << (*block[7]->As<ColumnUInt8>())[i] << " "
            //                 << (*block[8]->As<ColumnUInt16>())[i] << " "
            //                 << (*block[9]->As<ColumnUInt32>())[i] << " " 
            //                 << (*block[10]->As<ColumnUInt64>())[i] << " "
            //                 << (*block[11]->As<ColumnUInt128>())[i] << " "
            //                 << (*block[12]->As<ColumnUInt256>())[i] << "\n";
            // }
            std::cout << PrettyPrintBlock{block} << std::endl;
        }
    );

    /// Delete table.
    client.Execute("DROP STREAM test_int");
}

inline void testDateType(Client& client) {
    Block b;

    /// Create a table.
    client.Execute("CREATE STREAM IF NOT EXISTS test_date (d date, d32 date32)");

    auto d = std::make_shared<ColumnDate>();
    auto d32 = std::make_shared<ColumnDate32>();
    d->Append(std::time(nullptr));
    d32->Append(std::time(nullptr));
    b.AppendColumn("d", d);
    b.AppendColumn("d32", d32);
    client.Insert("test_date", b);

    client.Select("SELECT d, d32 FROM test_date", [](const Block& block)
        {
            for (size_t c = 0; c < block.GetRowCount(); ++c) {

                auto print_value = [&](const auto& col) {
                    std::time_t t = col->At(c);
                    std::cerr << std::asctime(std::localtime(&t));
                    std::cerr << col->Timezone() << std::endl;
                };

                print_value(block[0]->As<ColumnDateTime>());
                print_value(block[1]->As<ColumnDateTime>());
            }
        }
    );

    /// Delete table.
    client.Execute("DROP STREAM test_date");
}

inline void testDateTimeType(Client& client) {
    Block b;

    /// Create a table.
    client.Execute("CREATE STREAM IF NOT EXISTS test_datetime (dt32 datetime, dt64 DateTime64(6))");
    
    auto dt32 = std::make_shared<ColumnDateTime>();
    auto dt64 = std::make_shared<ColumnDateTime64>(6);
    dt32->Append(std::time(nullptr));
    dt64->Append(std::time(nullptr) * 1000000 + 123456);

    b.AppendColumn("dt32", dt32);
    b.AppendColumn("dt64", dt64);

    client.Insert("test_datetime", b);

    client.Select("SELECT dt32, dt64 FROM test_datetime", [](const Block& block)
        {
            for (size_t c = 0; c < block.GetRowCount(); ++c) {
                auto print_value = [&](const auto& col) {
                    std::time_t t = col->At(c);
                    std::cerr << std::asctime(std::localtime(&t));
                    std::cerr << col->Timezone() << std::endl;
                };

                print_value(block[0]->As<ColumnDateTime>());
                print_value(block[1]->As<ColumnDateTime64>());

            }
        }
    );

    /// Delete table.
    client.Execute("DROP STREAM test_datetime");
}

inline void testDecimalType(Client& client) {
    Block b;

    /// Create a table.
    client.Execute(
        "CREATE TABLE IF NOT EXISTS "
        "test_cpp_decimal (id UInt64, d1 Decimal(9, 4), d2 Decimal(18, 9), d3 Decimal(38, 19), "
        "                         d4 Decimal32(4), d5 Decimal64(9), d6 Decimal128(19)) ");


    auto id = std::make_shared<ColumnUInt64>();
    auto d1 = std::make_shared<ColumnDecimal>(9, 4);
    auto d2 = std::make_shared<ColumnDecimal>(18, 9);
    auto d3 = std::make_shared<ColumnDecimal>(38, 19);
    auto d4 = std::make_shared<ColumnDecimal>(9, 4);
    auto d5 = std::make_shared<ColumnDecimal>(18, 9);
    auto d6 = std::make_shared<ColumnDecimal>(38, 19);

    EXPECT_THROW(
        d1->Append("1234567890123456789012345678901234567890"),
        std::runtime_error
    );
    EXPECT_THROW(
        d1->Append("123456789012345678901234567890123456.7890"),
        std::runtime_error
    );
    EXPECT_THROW(
        d1->Append("-1234567890123456789012345678901234567890"),
        std::runtime_error
    );
    EXPECT_THROW(
        d1->Append("12345678901234567890123456789012345678a"),
        std::runtime_error
    );
    EXPECT_THROW(
        d1->Append("12345678901234567890123456789012345678-"),
        std::runtime_error
    );
    EXPECT_THROW(
        d1->Append("1234.12.1234"),
        std::runtime_error
    );

    id->Append(1);
    d1->Append(123456789);
    d2->Append(123456789012345678);
    d3->Append(1234567890123456789);
    d4->Append(123456789);
    d5->Append(123456789012345678);
    d6->Append(1234567890123456789);

    id->Append(2);
    d1->Append(999999999);
    d2->Append(999999999999999999);
    d3->Append(999999999999999999);
    d4->Append(999999999);
    d5->Append(999999999999999999);
    d6->Append(999999999999999999);

    id->Append(3);
    d1->Append(-999999999);
    d2->Append(-999999999999999999);
    d3->Append(-999999999999999999);
    d4->Append(-999999999);
    d5->Append(-999999999999999999);
    d6->Append(-999999999999999999);

    // Check strings with decimal point
    id->Append(4);
    d1->Append("12345.6789");
    d2->Append("123456789.012345678");
    d3->Append("1234567890123456789.0123456789012345678");
    d4->Append("12345.6789");
    d5->Append("123456789.012345678");
    d6->Append("1234567890123456789.0123456789012345678");

    // Check strings with minus sign and without decimal point
    id->Append(5);
    d1->Append("-12345.6789");
    d2->Append("-123456789012345678");
    d3->Append("-12345678901234567890123456789012345678");
    d4->Append("-12345.6789");
    d5->Append("-123456789012345678");
    d6->Append("-12345678901234567890123456789012345678");

    id->Append(6);
    d1->Append("12345.678");
    d2->Append("123456789.0123456789");
    d3->Append("1234567890123456789.0123456789012345678");
    d4->Append("12345.6789");
    d5->Append("123456789.012345678");
    d6->Append("1234567890123456789.0123456789012345678");

    b.AppendColumn("id", id);
    b.AppendColumn("d1", d1);
    b.AppendColumn("d2", d2);
    b.AppendColumn("d3", d3);
    b.AppendColumn("d4", d4);
    b.AppendColumn("d5", d5);
    b.AppendColumn("d6", d6);



    client.Insert("test_cpp_decimal", b);

    client.Select("SELECT * FROM test_cpp_decimal", [](const Block& block)
        {
            std::cout << PrettyPrintBlock{block} << std::endl;
        }
    );


    /// Delete table.
    client.Execute("DROP STREAM test_cpp_decimal");
}

inline void testEnumType(Client& client) {
    /// Create a table.
     client.Execute("CREATE STREAM IF NOT EXISTS test_enums (id uint64, e1 enum8('One' = 1, 'Two' = 2)), e2 enum16('A' = 1, 'B' = 2))");

    /// Insert some values.
    {
        Block block;

        auto id = std::make_shared<ColumnUInt64>();
        id->Append(1);
        id->Append(2);

        auto e1 = std::make_shared<ColumnEnum8>(Type::CreateEnum8({{"One", 1}, {"Two", 2}}));
        e1->Append(1);
        e1->Append("Two");

        auto e2 = std::make_shared<ColumnEnum8>(Type::CreateEnum16({{"A", 1}, {"B", 2}}));
        e2->Append("A");
        e2->Append("B");

        block.AppendColumn("id", id);
        block.AppendColumn("e1", e1);
        block.AppendColumn("e2", e2);

        client.Insert("test_enums", block);
    }

    /// Select values inserted in the previous step.
    client.Select("SELECT id, e1, e2 FROM test_enums", [](const Block& block)
        {
            for (Block::Iterator bi(block); bi.IsValid(); bi.Next()) {
                std::cout << bi.Name() << " ";
            }
            std::cout << std::endl;

            for (size_t i = 0; i < block.GetRowCount(); ++i) {
                std::cout << (*block[0]->As<ColumnUInt64>())[i] << " "
                          << (*block[1]->As<ColumnEnum8>()).NameAt(i) << " "
                          << (*block[2]->As<ColumnEnum16>()).NameAt(i) << "\n";
            }
        }
    );

    /// Delete table.
    client.Execute("DROP STREAM test_enums");
}

inline void testFloatType(Client& client) {
    /// Create a table.
     client.Execute("CREATE STREAM IF NOT EXISTS test_float (f32 float32, f64 float64)");

    /// Insert some values.
    {
        Block block;

        auto f32 = std::make_shared<ColumnFloat32>();
        f32->Append(1.000001);
        f32->Append(9999.99999999);

        auto f64 = std::make_shared<ColumnFloat64>();
        f64->Append(1.000000000000000000001);
        f64->Append(999999999999999999.999999999999999999);

        block.AppendColumn("f32", f32);
        block.AppendColumn("f64", f64);

        client.Insert("test_float", block);
    }

    /// Select values inserted in the previous step.
    client.Select("SELECT f32, f64 FROM test_float", [](const Block& block)
        {
            // for (Block::Iterator bi(block); bi.IsValid(); bi.Next()) {
            //     std::cout << bi.Name() << " ";
            // }
            // std::cout << std::endl;

            // for (size_t i = 0; i < block.GetRowCount(); ++i) {
            //     std::cout << (*block[0]->As<ColumnFloat32>())[i] << " "
            //               << (*block[1]->As<ColumnFloat64>())(i) << "\n";
            // }
            std::cout << PrettyPrintBlock{block} << std::endl;
        }
    );

    /// Delete table.
    client.Execute("DROP STREAM test_float");
}

inline void testIPType(Client & client) {
    /// Create a table.
    // client.Execute("CREATE STREAM IF NOT EXISTS test_ips (id uint64, v4 ipv4, v6 ipv6)");

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
        client.Select("SELECT id, v4, v6 FROM test_ips", [&](const Block& block)
        {

            // for (Block::Iterator bi(block); bi.IsValid(); bi.Next()) {
            //     std::cout << bi.Name() << " ";
            // }
            // std::cout << std::endl;

            for (size_t i = 0; i < block.GetRowCount(); ++i) {
                std::cout << (*block[0]->As<ColumnUInt64>())[i] << " "
                          << (*block[1]->As<ColumnIPv4>()).AsString(i) << " (" << (*block[1]->As<ColumnIPv4>())[i].s_addr << ") "
                          << (*block[2]->As<ColumnIPv6>()).AsString(i) << "\n";
            }

        }
    );

    /// Delete table.
    client.Execute("DROP STREAM test_ips");
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

    /// Create a table.
    client.Execute("CREATE STREAM IF NOT EXISTS test_array (arr array(uint64))");

    auto arr = std::make_shared<ColumnArray>(std::make_shared<ColumnUInt64>());

    auto id = std::make_shared<ColumnUInt64>();
    id->Append(1);
    arr->AppendAsColumn(id);

    id->Append(3);
    arr->AppendAsColumn(id);

    id->Append(7);
    arr->AppendAsColumn(id);

    id->Append(9);
    arr->AppendAsColumn(id);

    b.AppendColumn("arr", arr);
    client.Insert("test_array", b);

    client.Select("SELECT arr FROM test_array", [](const Block& block)
        {
            for (size_t c = 0; c < block.GetRowCount(); ++c) {
                auto col = block[0]->As<ColumnArray>()->GetAsColumn(c);
                for (size_t i = 0; i < col->Size(); ++i) {
                    std::cerr << (int)(*col->As<ColumnUInt64>())[i] << " ";
                }
                std::cerr << std::endl;
            }
        }
    );

    /// Delete table.
    client.Execute("DROP STREAM test_array");
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
    auto m1 = std::make_shared<ColumnMapT<ColumnUInt64, ColumnString>>(
    std::make_shared<ColumnUInt64>(),
    std::make_shared<ColumnString>());

    std::map<uint64_t, std::string> val;
    val[0] = "123";
    val[1] = "abc";

    (*m1).Append(val);

    block.AppendColumn("m1",m1);
    
    client.Execute("DROP STREAM IF EXISTS test_map");
    client.Execute("CREATE STREAM IF NOT EXISTS test_map (m1 map(uint64, string))");
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
    testIntType(client);
    // tArrayType(client);
    // CancelableExample(client);
    // testDateType(client);
    // testDateTimeType(client);
    // tArrayType(client);
    // CancelableExample(client);
    // testEnumType(client);
    // ExecptionExample(client);
    // GenericExample(client);
    // testIPType(client);
    // MultitestArrayType(client);
    // NullableExample(client);
    // NumbersExample(client);
    // SelectNull(client);
    // ShowTables(client);
}

// int main() {
//     try {
//         const auto localHostEndpoint = ClientOptions()
//                 .SetHost(   getEnvOrDefault("CLICKHOUSE_HOST",     "localhost"))
//                 .SetPort(   getEnvOrDefault<size_t>("CLICKHOUSE_PORT",     "9000"))
//                 .SetEndpoints({   {"asasdasd", 9000}
//                                  ,{"localhost"}
//                                  ,{"noalocalhost", 9000}
//                                })
//                 .SetUser(           getEnvOrDefault("CLICKHOUSE_USER",     "default"))
//                 .SetPassword(       getEnvOrDefault("CLICKHOUSE_PASSWORD", ""))
//                 .SetDefaultDatabase(getEnvOrDefault("CLICKHOUSE_DB",       "default"));

//         {
//             Client client(ClientOptions(localHostEndpoint)
//                     .SetPingBeforeQuery(true));
//             RunTests(client);
//             std::cout << "current endpoint : " <<  client.GetCurrentEndpoint().value().host << "\n";
//         }

//         {
//             Client client(ClientOptions(localHostEndpoint)
//                     .SetPingBeforeQuery(true)
//                     .SetCompressionMethod(CompressionMethod::LZ4));
//             RunTests(client);
//         }
//     } catch (const std::exception& e) {
//         std::cerr << "exception : " << e.what() << std::endl;
//     }

//     return 0;
// }

int main()
{
    /// Initialize client connection.
    try {
        const auto localHostEndpoint = ClientOptions()
                .SetHost(   getEnvOrDefault("CLICKHOUSE_HOST",     "localhost"))
                .SetPort(   getEnvOrDefault<size_t>("CLICKHOUSE_PORT",     "8463"));

        {
            Client client(ClientOptions(localHostEndpoint)
                    .SetPingBeforeQuery(true));
            RunTests(client);
            std::cout << "current endpoint : " <<  client.GetCurrentEndpoint().value().host << "\n";
        }

        // {
        //     Client client(ClientOptions(localHostEndpoint)
        //             .SetPingBeforeQuery(true)
        //             .SetCompressionMethod(CompressionMethod::LZ4));
        //     RunTests(client);
        // }
    } catch (const std::exception& e) {
        std::cerr << "exception : " << e.what() << std::endl;
    }


    return 0;
}