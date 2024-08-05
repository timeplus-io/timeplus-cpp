#include "TestFunctions.h"


void testIntType(Client& client) {

    client.Execute("DROP STREAM IF EXISTS test_insert_int");
    client.Execute("CREATE STREAM IF NOT EXISTS test_insert_int "
    "(id uint64, int8_val int8, int16_val int16, int32_val int32, int64_val int64, int128_val int128, int256_val int256, uint128_val uint128, uint256_val uint256) ENGINE = Memory");

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

    int8_col->Append(INT8_MIN);
    int8_col->Append(0);
    int8_col->Append(INT8_MAX);

    int16_col->Append(INT16_MIN);
    int16_col->Append(0);
    int16_col->Append(INT16_MAX);

    int32_col->Append(INT32_MIN);
    int32_col->Append(0);
    int32_col->Append(INT32_MAX);

    int64_col->Append(INT64_MIN);
    int64_col->Append(0);
    int64_col->Append(INT64_MAX);

    int128_col->Append(min_val_128);
    int128_col->Append(static_cast<Int128>(0));
    int128_col->Append(max_val_128);

    int256_col->Append(min_val_256);
    int256_col->Append(static_cast<Int256>(0));
    int256_col->Append(max_val_256);

    uint128_col->Append(static_cast<UInt128>(0));
    uint128_col->Append(max_val_u128/2);
    uint128_col->Append(max_val_u128);

    uint256_col->Append(static_cast<UInt256>(0));
    uint256_col->Append(max_val_u256/2);
    uint256_col->Append(max_val_u256);


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

    client.Select("SELECT * FROM test_insert_int", [](const Block& block)
    {
        for (size_t c = 0; c < block.GetRowCount(); ++c) {
            auto col0 = block[0]->As<ColumnUInt64>()->At(c);
            std::cout<< col0 <<std::endl;
            auto col1 = block[1]->As<ColumnInt8>()->At(c);
            std::cout<< col1 <<std::endl;
        }
    }
    );


}


void testDateTimeType(Client& client) {
    Block b;

    client.Execute("DROP STREAM IF EXISTS test_datetime");
    client.Execute("CREATE STREAM IF NOT EXISTS test_datetime (d date, d32 date32, dt32 datetime, dt64 datetime64(6, 'Asia/Shanghai')) ENGINE = Memory");
    
    auto d = std::make_shared<ColumnDate>();
    auto d32 = std::make_shared<ColumnDate32>();
    auto dt32 = std::make_shared<ColumnDateTime>();
    auto dt64 = std::make_shared<ColumnDateTime64>(6);

    auto now = getCurrentTimeNanoseconds(6);

    d->Append(std::time(nullptr));
    d32->Append(std::time(nullptr));
    dt32->Append(std::time(nullptr));
    dt64->Append(now);


    d->Append(0);
    d32->Append(0);
    dt32->Append(0);
    dt64->Append(0);

    d->Append(100);
    d32->Append(4294967295);
    dt32->Append(4294967295);
    dt64->Append(95178239999999);

    b.AppendColumn("d", d);
    b.AppendColumn("d32", d32);
    b.AppendColumn("dt32", dt32);
    b.AppendColumn("dt64", dt64);

    client.Insert("test_datetime", b);

    client.Select("SELECT d, d32, dt32, dt64 FROM test_datetime", [](const Block& block)
        {
                for (size_t c = 0; c < block.GetRowCount(); ++c) {
                    auto print_value = [&](const auto& col){
                        std::time_t t = col->At(c);
                        std::cerr << formatTimestamp(t) << std::endl;
                    };
                    auto print_value_dt = [&](const auto& col) {
                        std::time_t t = col->At(c);
                        std::cerr << formatTimestamp(t, 0);
                        std::cerr << col->Timezone() << std::endl;
                    };
                    auto print_value_dt64 = [&](const auto& col) {
                        std::time_t t = col->At(c);
                        std::cerr << formatTimestamp(t, 6);
                        std::cerr << col->Timezone() << std::endl;
                    };

                    print_value(block[0]->As<ColumnDate>());
                    print_value(block[1]->As<ColumnDate32>());
                    print_value_dt(block[2]->As<ColumnDateTime>());
                    print_value_dt64(block[3]->As<ColumnDateTime64>());

            }
        }
    );

    client.Execute("DROP STREAM IF EXISTS test_datetime");
}

void testDecimalType(Client& client) {
    Block b;

    client.Execute("DROP STREAM IF EXISTS test_decimal");
    client.Execute(
        "CREATE STREAM IF NOT EXISTS "
        "test_decimal (id uint64, d1 decimal(9, 4), d2 decimal(18, 9), d3 decimal(38, 19), "
        "                         d4 decimal32(4), d5 decimal64(9), d6 decimal128(19), d7 decimal(76, 38), d8 decimal256(38)) ENGINE = Memory");


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

    id->Append(4);
    d1->Append(std::string("12345.6789"));
    d2->Append(std::string("123456789.012345678"));
    d3->Append(std::string("1234567890123456789.0123456789012345678"));
    d4->Append(std::string("12345.6789"));
    d5->Append(std::string("123456789.012345678"));
    d6->Append(std::string("1234567890123456789.0123456789012345678"));
    d7->Append(std::string("12345678901234567890123456789012345678.90123456789012345678901234567890123456"));
    d8->Append(std::string("12345678901234567890123456789012345678.90123456789012345678901234567890123456"));

    id->Append(5);
    d1->Append(std::string("-1234.56789"));
    d2->Append(std::string("-1234.56789012345678"));
    d3->Append(std::string("-1234.5678901234567890123456789012345678"));
    d4->Append(std::string("-1234.56789"));
    d5->Append(std::string("-1234.56789012345678"));
    d6->Append(std::string("-1234.5678901234567890123456789012345678"));
    d7->Append(std::string("-1234567890123456789012.345678901234567890123456789012345678901234567890123456"));
    d8->Append(std::string("-12345678901234567890123456789012345678.90123456789012345678901234567890123456"));


    b.AppendColumn("id", id);
    b.AppendColumn("d1", d1);
    b.AppendColumn("d2", d2);
    b.AppendColumn("d3", d3);
    b.AppendColumn("d4", d4);
    b.AppendColumn("d5", d5);
    b.AppendColumn("d6", d6);
    b.AppendColumn("d7", d7);
    b.AppendColumn("d8", d8);

    client.Insert("test_decimal", b);

    client.Select("SELECT * FROM test_decimal", [](const Block& block)
        {
            for (size_t c = 0; c < block.GetRowCount(); ++c) {
                auto col0 = block[0]->As<ColumnUInt64>()->At(c);
                std::cout<< col0 <<std::endl;
                auto col1 = block[1]->As<ColumnDecimal>()->At(c);
                std::cout<< col1 <<std::endl;
            }       
        }
    );


    client.Execute("DROP STREAM IF EXISTS test_decimal");
}

void testEnumType(Client& client) {

    client.Execute("DROP STREAM IF EXISTS test_enums");
    client.Execute("CREATE STREAM IF NOT EXISTS test_enums (id uint64, e1 enum8('Min' = -128, 'Max' = 127), e2 enum16('Mn' = -32768, 'Mx' = 32767)) ENGINE = Memory");

    {
        Block block;

        auto id = std::make_shared<ColumnUInt64>();
        id->Append(1);
        id->Append(2);

        auto e1 = std::make_shared<ColumnEnum8>(Type::CreateEnum8({{"Min", -128}, {"Max", 127}}));
        e1->Append(-128);
        e1->Append("Max");

        auto e2 = std::make_shared<ColumnEnum16>(Type::CreateEnum16({{"Mn", -32768}, {"Mx", 32767}}));
        e2->Append("Mn");
        e2->Append("Mx");

        block.AppendColumn("id", id);
        block.AppendColumn("e1", e1);
        block.AppendColumn("e2", e2);

        client.Insert("test_enums", block);
    }

    client.Select("SELECT id, e1, e2 FROM test_enums", [](const Block& block)
        {
            for (size_t c = 0; c < block.GetRowCount(); ++c) {
                auto col0 = block[0]->As<ColumnUInt64>()->At(c);
                std::cout<< col0 <<std::endl;
                auto col1 = block[1]->As<ColumnEnum8>()->At(c);
                std::cout<< col1 <<std::endl;
                auto col2 = block[2]->As<ColumnEnum16>()->At(c);
                std::cout<< col2 <<std::endl;
            }
        }
    );

    client.Execute("DROP STREAM IF EXISTS test_enums");
}

void testFloatType(Client& client) {
    client.Execute("DROP STREAM IF EXISTS test_float");
    client.Execute("CREATE STREAM IF NOT EXISTS test_float (f32 float32, f64 float64) ENGINE = Memory");

    {
        Block block;

        float max_float32 = std::numeric_limits<float>::max();
        float min_float32 = std::numeric_limits<float>::min();
        
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

    client.Select("SELECT f32, f64 FROM test_float", [](const Block& block)
        {
            for (size_t c = 0; c < block.GetRowCount(); ++c) {
                auto col0 = block[0]->As<ColumnFloat32>()->At(c);
                std::cout<< col0 <<std::endl;
            }
        }
    );

    client.Execute("DROP STREAM IF EXISTS test_float");
}

void testIPType(Client & client) {
    client.Execute("DROP STREAM IF EXISTS test_ips");
    client.Execute("CREATE STREAM IF NOT EXISTS test_ips (id uint64, v4 ipv4, v6 ipv6) ENGINE = Memory");

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

    client.Select("SELECT id, v4, v6 FROM test_ips", [&](const Block& block)
        {
            for (size_t c = 0; c < block.GetRowCount(); ++c) {
                auto col0 = block[0]->As<ColumnUInt64>()->At(c);
                std::cout<< col0 <<std::endl;
                auto col1 = block[1]->As<ColumnIPv4>()->At(c);
                std::cout<< col1 <<std::endl;
                auto col2 = block[2]->As<ColumnIPv6>()->At(c);
                std::cout<< col2 <<std::endl;
            }
        }
    );

    client.Execute("DROP STREAM IF EXISTS test_ips");
}

void testArrayType(Client& client) {
    Block b;

    client.Execute("DROP STREAM IF EXISTS test_array_types");
    client.Execute("CREATE STREAM IF NOT EXISTS test_array_types "
                   "(arr_int128 array(int128), arr_uint128 array(uint128), "
                   "arr_decimal array(decimal(10,2)), arr_float32 array(float32), arr_float64 array(float64), "
                   "arr_ip array(ipv4), arr_enum8 array(enum8('First' = 1, 'Second' = 2))) ENGINE = Memory");

    auto arr_int128 = std::make_shared<ColumnArray>(std::make_shared<ColumnInt128>());
    auto arr_uint128 = std::make_shared<ColumnArray>(std::make_shared<ColumnUInt128>());
    auto arr_decimal = std::make_shared<ColumnArray>(std::make_shared<ColumnDecimal>(10,2));
    auto arr_float32 = std::make_shared<ColumnArray>(std::make_shared<ColumnFloat32>());
    auto arr_float64 = std::make_shared<ColumnArray>(std::make_shared<ColumnFloat64>());
    auto arr_ip = std::make_shared<ColumnArray>(std::make_shared<ColumnIPv4>());
    auto arr_enum8 = std::make_shared<ColumnArray>(std::make_shared<ColumnEnum8>(Type::CreateEnum8({{"First", 1}, {"Second", 2}})));


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

    client.Select("SELECT * FROM test_array_types", [](const Block& block)
        {
            std::cout << PrettyPrintBlock{block} << std::endl;
        }
    );

    client.Execute("DROP STREAM IF EXISTS test_array_types");
}

void testUUIDType(Client& client) {
    client.Execute("DROP STREAM IF EXISTS test_uuid");
    client.Execute("CREATE STREAM IF NOT EXISTS test_uuid (uu uuid) ENGINE = Memory");

    {
        Block block;
        

        auto uuid_max = UUID({0xffffffffffffffffllu, 0xffffffffffffffffllu});
        auto uuid_min = UUID({0x0llu, 0x0llu});


        auto uu = std::make_shared<ColumnUUID>();
        uu->Append(uuid_min);
        uu->Append(uuid_max);

        block.AppendColumn("uu", uu);

        client.Insert("test_uuid", block);
    }

    client.Select("SELECT uu FROM test_uuid", [](const Block& block)
        {
            for (size_t c = 0; c < block.GetRowCount(); ++c) {
                auto col0 = block[0]->As<ColumnUUID>()->At(c);
                std::cout<< col0 <<std::endl;
            }
        }
    );

    client.Execute("DROP STREAM IF EXISTS test_uuid");
}

void testStringType(Client& client){
    Block block;

    auto s1 = std::make_shared<ColumnString>();
    auto s2 = std::make_shared<ColumnFixedString>(1);
    auto s3 = std::make_shared<ColumnFixedString>(10000);

    s1->Append("string1");
    s2->Append("s");
    s3->Append("This is a so-long string");


    block.AppendColumn("s1", s1);
    block.AppendColumn("s2", s2);
    block.AppendColumn("s3", s3);

    client.Execute("DROP STREAM IF EXISTS test_string");
    client.Execute("CREATE STREAM IF NOT EXISTS test_string(s1 string, s2 fixed_string(1), s3 fixed_string(10000)) ENGINE = Memory");
    client.Insert("test_string", block);

    client.Select("SELECT * FROM test_string", [](const Block& block)
        {
            for (size_t c = 0; c < block.GetRowCount(); ++c) {
                auto col0 = block[0]->As<ColumnString>()->At(c);
                std::cout<< col0 <<std::endl;
                auto col1 = block[1]->As<ColumnFixedString>()->At(c);
                std::cout<< col1 <<std::endl;
                auto col2 = block[2]->As<ColumnFixedString>()->At(c);
                std::cout<< col2 <<std::endl;
            }
        }
    );

    client.Execute("DROP STREAM IF EXISTS test_string");
}

void testNullabletype(Client& client) {

    client.Execute("DROP STREAM IF EXISTS test_nullable");
    client.Execute("CREATE STREAM IF NOT EXISTS test_nullable (id nullable(uint64), i256 nullable(int256), "
    "date nullable(date), datetime nullable(datetime), deci nullable(decimal(9,4)), fl nullable(float32), "
    "e1 nullable(enum8('a' = 1, 'b' = 2)), ip nullable(ipv4), st nullable(string), fst nullable(fixed_string(4)), "
    "uu nullable(uuid)) ENGINE = Memory");

    {
        Block block;

        {
            auto id = std::make_shared<ColumnUInt64>();
            id->Append(1);
            id->Append(2);

            auto nulls = std::make_shared<ColumnUInt8>();
            nulls->Append(0);
            nulls->Append(1);

            block.AppendColumn("id", std::make_shared<ColumnNullable>(id, nulls));
        }

        {
            auto i256 = std::make_shared<ColumnInt256>();
            i256->Append(1);
            i256->Append(2);

            auto nulls = std::make_shared<ColumnUInt8>();
            nulls->Append(0);
            nulls->Append(1);

            block.AppendColumn("i256", std::make_shared<ColumnNullable>(i256, nulls));
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

        {
            auto datetime = std::make_shared<ColumnDateTime>();
            datetime->Append(std::time(nullptr));
            datetime->Append(std::time(nullptr));

            auto nulls = std::make_shared<ColumnUInt8>();
            nulls->Append(0);
            nulls->Append(1);

            block.AppendColumn("datetime", std::make_shared<ColumnNullable>(datetime, nulls));
        }

        {
            auto deci = std::make_shared<ColumnDecimal>(9,4);
            deci->Append(123.456);
            deci->Append(789.012);

            auto nulls = std::make_shared<ColumnUInt8>();
            nulls->Append(0);
            nulls->Append(1);

            block.AppendColumn("deci", std::make_shared<ColumnNullable>(deci, nulls));
        }        

        {
            auto fl = std::make_shared<ColumnFloat32>();
            fl->Append(123.456);
            fl->Append(789.012);

            auto nulls = std::make_shared<ColumnUInt8>();
            nulls->Append(0);
            nulls->Append(1);

            block.AppendColumn("fl", std::make_shared<ColumnNullable>(fl, nulls));
        }  

        {
            auto ip = std::make_shared<ColumnIPv4>();
            ip->Append("127.0.0.1");
            ip->Append(11111111);

            auto nulls = std::make_shared<ColumnUInt8>();
            nulls->Append(0);
            nulls->Append(1);

            block.AppendColumn("ip", std::make_shared<ColumnNullable>(ip, nulls));
        }       

        {
            auto e1 = std::make_shared<ColumnEnum8>(Type::CreateEnum8({{"a", 1}, {"b", 2}}));
            e1->Append(1);
            e1->Append("b");

            auto nulls = std::make_shared<ColumnUInt8>();
            nulls->Append(0);
            nulls->Append(1);

            block.AppendColumn("e1", std::make_shared<ColumnNullable>(e1, nulls));
        }      

        {
            auto st = std::make_shared<ColumnString>();
            st->Append("123.456");
            st->Append("789.012");

            auto nulls = std::make_shared<ColumnUInt8>();
            nulls->Append(0);
            nulls->Append(1);

            block.AppendColumn("st", std::make_shared<ColumnNullable>(st, nulls));
        }  

        {
            auto fst = std::make_shared<ColumnFixedString>(4);
            fst->Append("1234");
            fst->Append("5678");

            auto nulls = std::make_shared<ColumnUInt8>();
            nulls->Append(1);
            nulls->Append(0);

            block.AppendColumn("fst", std::make_shared<ColumnNullable>(fst, nulls));
        }  

        {
            auto uu = std::make_shared<ColumnUUID>();
            uu->Append(123456);
            uu->Append(789012);

            auto nulls = std::make_shared<ColumnUInt8>();
            nulls->Append(0);
            nulls->Append(1);

            block.AppendColumn("uu", std::make_shared<ColumnNullable>(uu, nulls));
        }  

        client.Insert("test_nullable", block);
    }

    client.Select("SELECT * FROM test_nullable", [](const Block& block)
        {
            for (size_t c = 0; c < block.GetRowCount(); ++c) {
                auto col_id   = block[0]->As<ColumnNullable>();
                std::cout<< col_id->Nested()->As<ColumnUInt64>()->At(c) <<std::endl;
            }
        }
    );

    client.Execute("DROP STREAM IF EXISTS test_nullable");
}

void testMultitesArrayType(Client& client) {

    Block block;

    client.Execute("DROP STREAM IF EXISTS test_multiarray");
    client.Execute("CREATE STREAM IF NOT EXISTS test_multiarray(m1 map(low_cardinality(fixed_string(10)), array(nullable(uint64)))) ENGINE = Memory");

    using NullableUInt64 = std::optional<uint64_t>;
    using ArrayOfNullableUInt64 = std::vector<NullableUInt64>;

    std::map<std::string, ArrayOfNullableUInt64> val1;
    val1["key1"] = {1, 2, std::nullopt, 4};
    val1["key2"] = {std::nullopt, 6, 7, 8};

    auto m1 = std::make_shared<ColumnMapT<ColumnLowCardinalityT<ColumnFixedString>, ColumnArrayT<ColumnNullableT<ColumnUInt64>>>>(
        std::make_shared<ColumnLowCardinalityT<ColumnFixedString>>(10),
        std::make_shared<ColumnArrayT<ColumnNullableT<ColumnUInt64>>>()
    );

    m1->Append(val1);
    block.AppendColumn("m1", m1);

    client.Insert("test_multiarray", block);

    client.Select("SELECT * FROM test_multiarray", [](const Block& block)
        {
            for (size_t c = 0; c < block.GetRowCount(); ++c) {
                // auto col0 = block[0]->As<ColumnMapT>()->GetAsColumn(c);
            }
        }
    );

    client.Execute("DROP STREAM IF EXISTS test_multiarray");
}

void testLowCardinalityStringType(Client& client){
    Block block;

    client.Execute("DROP STREAM IF EXISTS test_low_cardinality");
    client.Execute("CREATE STREAM IF NOT EXISTS test_low_cardinality(ls1 low_cardinality(fixed_string(10)), ls2 low_cardinality(string)) ENGINE = Memory");


    auto ls1 = std::make_shared<ColumnLowCardinalityT<ColumnFixedString>>(10);

    auto ls2 = std::make_shared<ColumnLowCardinalityT<ColumnString>>();

    ls1->Append("1");

    ls2->Append("2");

    block.AppendColumn("ls1", ls1);
    block.AppendColumn("ls2", ls2);


    client.Insert("test_low_cardinality", block);

    client.Select("SELECT * FROM test_low_cardinality", [](const Block& block)
        {
            for (size_t c = 0; c < block.GetRowCount(); ++c) {
                auto col1 = block[0]->As<ColumnLowCardinalityT<ColumnFixedString>>()->At(c);
                std::cout<< col1 <<std::endl;
                auto col2 = block[1]->As<ColumnLowCardinalityT<ColumnString>>()->At(c);
                std::cout<< col2 <<std::endl;
            }
        }
    );

    client.Execute("DROP STREAM IF EXISTS test_low_cardinality");
}

void testMapType(Client& client){

    client.Execute("DROP STREAM IF EXISTS map_example");
    client.Execute("CREATE STREAM IF NOT EXISTS map_example "
                   "(id uint64, mp map(uint64, string)) ENGINE = Memory");
    using Mapt = ColumnMapT<ColumnUInt64, ColumnString>;
    {
        Block block;

        auto id = std::make_shared<ColumnUInt64>();
        id->Append(1);

        auto mp = std::make_shared<Mapt>(std::make_shared<ColumnUInt64>(), std::make_shared<ColumnString>());

        std::map<uint64_t, std::string> row;
        row[1] = "hello";
        row[2] = "world";
        mp->Append(row);

        block.AppendColumn("id", id);
        block.AppendColumn("mp", mp);

        client.Insert("map_example", block);
    }

    client.Select("SELECT * FROM map_example", [](const Block& block)
        {
            for (size_t c = 0; c < block.GetRowCount(); ++c) {
                auto col1 = block[0]->As<ColumnUInt64>()->At(c);
                std::cout<< col1 <<std::endl;

                auto col2 = block[1]->As<ColumnMap>();
                const auto tuples = col2->GetAsColumn(c)->As<ColumnTuple>();
                for(size_t i = 0; i < tuples->Size(); i++){
                    std::cout<< (*tuples)[0]->As<ColumnUInt64>()->At(i)<<std::endl;
                    std::cout<< (*tuples)[1]->As<ColumnString>()->At(i)<<std::endl;
                }

            }

        }
    );

    client.Execute("DROP STREAM IF EXISTS map_example");
}

void testTupleType(Client& client){

    client.Execute("DROP STREAM IF EXISTS tuple_example");
    client.Execute("CREATE STREAM IF NOT EXISTS tuple_example "
                   "(id uint64, tup tuple(uint64, string)) ENGINE = Memory");

    {
        Block block;

        auto id = std::make_shared<ColumnUInt64>();
        id->Append(1);


        auto tupls = std::make_shared<ColumnTuple>(std::vector<ColumnRef>{
            std::make_shared<ColumnUInt64>(),
            std::make_shared<ColumnString>()}
        );

        (*tupls)[0]->As<ColumnUInt64>()->Append(1);
        (*tupls)[1]->As<ColumnString>()->Append("good");

        block.AppendColumn("id", id);
        block.AppendColumn("tup", tupls);


        client.Insert("tuple_example", block);
    }

    client.Select("SELECT * FROM tuple_example", [](const Block& block)
        {
            for (size_t c = 0; c < block.GetRowCount(); ++c) {
                auto col1 = block[0]->As<ColumnUInt64>()->At(c);
                std::cout<< col1 <<std::endl;

                auto col2 = block[1]->As<ColumnTuple>();
                for(size_t i = 0; i < col2->Size(); i++){
                    std::cout<< (*col2)[0]->As<ColumnUInt64>()->At(i)<<std::endl;
                    std::cout<< (*col2)[1]->As<ColumnString>()->At(i)<<std::endl;
                }

            }
        }
    );

    client.Execute("DROP STREAM IF EXISTS tuple_example");
}

void testNestedType(Client& client){

    client.Execute("DROP STREAM IF EXISTS nested_example");
    client.Execute("CREATE STREAM IF NOT EXISTS nested_example(n nested(id uint64, f float32)) ENGINE = Memory");


    {
        Block block;
        auto nested_id = std::make_shared<ColumnArray>(std::make_shared<ColumnUInt64>());
        auto nested_f = std::make_shared<ColumnArray>(std::make_shared<ColumnFloat32>());

        std::vector<uint64_t> id_values{1, 2};
        nested_id->As<ColumnArray>()->AppendAsColumn(std::make_shared<ColumnUInt64>(id_values));

        std::vector<float> f_values{1.1, 2.2};
        nested_f->As<ColumnArray>()->AppendAsColumn(std::make_shared<ColumnFloat32>(f_values));

        block.AppendColumn("n.id", nested_id);
        block.AppendColumn("n.f", nested_f);

        client.Insert("nested_example", block);
    }

    client.Select("SELECT * FROM nested_example", [](const Block& block)
        {
            for (size_t c = 0; c < block.GetRowCount(); ++c) {
                auto colid = block[0]->As<ColumnArray>()->GetAsColumn(c)->As<ColumnUInt64>();
                for(size_t i= 0; i<colid->Size(); ++i){
                    std::cout<< colid->At(i) <<std::endl;
                }

                auto colf = block[1]->As<ColumnArray>()->GetAsColumn(c)->As<ColumnFloat32>();
                for(size_t i= 0; i<colf->Size(); ++i){
                    std::cout<< colf->At(i) <<std::endl;
                }
            }
        }
    );

    client.Execute("DROP STREAM IF EXISTS nested_example");
}
