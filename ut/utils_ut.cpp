#include <gtest/gtest.h>
#include "ut/value_generators.h"
#include "utils.h"
#include <timeplus/block.h>
#include <timeplus/columns/numeric.h>

#include <iostream>
#include <numeric>
#include <limits>
#include <optional>
#include <vector>
using namespace timeplus;

TEST(CompareRecursive, CompareValues) {
    EXPECT_TRUE(CompareRecursive(1, 1));
    EXPECT_TRUE(CompareRecursive(1.0f, 1.0f));
    EXPECT_TRUE(CompareRecursive(1.0, 1.0));
    EXPECT_TRUE(CompareRecursive(1.0L, 1.0L));

    EXPECT_TRUE(CompareRecursive("1.0L", "1.0L"));
    EXPECT_TRUE(CompareRecursive(std::string{"1.0L"}, std::string{"1.0L"}));
    EXPECT_TRUE(CompareRecursive(std::string_view{"1.0L"}, std::string_view{"1.0L"}));
}

TEST(CompareRecursive, CompareContainers) {
    EXPECT_TRUE(CompareRecursive(std::vector<int>{1, 2, 3}, std::vector<int>{1, 2, 3}));
    EXPECT_TRUE(CompareRecursive(std::vector<int>{}, std::vector<int>{}));

    EXPECT_FALSE(CompareRecursive(std::vector<int>{1, 2, 3}, std::vector<int>{1, 2, 4}));
    EXPECT_FALSE(CompareRecursive(std::vector<int>{1, 2, 3}, std::vector<int>{1, 2}));

    // That would cause compile-time error:
    // EXPECT_FALSE(CompareRecursive(std::array{1, 2, 3}, 1));
}


TEST(CompareRecursive, CompareNestedContainers) {
    EXPECT_TRUE(CompareRecursive(
        std::vector<std::vector<int>>{{{1, 2, 3}, {4, 5, 6}}},
        std::vector<std::vector<int>>{{{1, 2, 3}, {4, 5, 6}}}));

    EXPECT_TRUE(CompareRecursive(
        std::vector<std::vector<int>>{{{1, 2, 3}, {4, 5, 6}, {}}},
        std::vector<std::vector<int>>{{{1, 2, 3}, {4, 5, 6}, {}}}));

    EXPECT_TRUE(CompareRecursive(
        std::vector<std::vector<int>>{{{}}},
        std::vector<std::vector<int>>{{{}}}));

    EXPECT_FALSE(CompareRecursive(std::vector<std::vector<int>>{{1, 2, 3}, {4, 5, 6}}, std::vector<std::vector<int>>{{1, 2, 3}, {4, 5, 7}}));
    EXPECT_FALSE(CompareRecursive(std::vector<std::vector<int>>{{1, 2, 3}, {4, 5, 6}}, std::vector<std::vector<int>>{{1, 2, 3}, {4, 5}}));
    EXPECT_FALSE(CompareRecursive(std::vector<std::vector<int>>{{1, 2, 3}, {4, 5, 6}}, std::vector<std::vector<int>>{{1, 2, 3}, {}}));
    EXPECT_FALSE(CompareRecursive(std::vector<std::vector<int>>{{1, 2, 3}, {4, 5, 6}}, std::vector<std::vector<int>>{{}}));
}

TEST(StringUtils, UUID) {
    const timeplus::UUID& uuid{0x0102030405060708, 0x090a0b0c0d0e0f10};
    const std::string uuid_string = "01020304-0506-0708-090a-0b0c0d0e0f10";
    EXPECT_EQ(ToString(uuid), uuid_string);
}

TEST(CompareRecursive, Nan) {
    /// Even though NaN == NaN is FALSE, CompareRecursive must compare those as TRUE.

    const auto NaNf = std::numeric_limits<float>::quiet_NaN();
    const auto NaNd = std::numeric_limits<double>::quiet_NaN();

    EXPECT_TRUE(CompareRecursive(NaNf, NaNf));
    EXPECT_TRUE(CompareRecursive(NaNd, NaNd));

    EXPECT_TRUE(CompareRecursive(NaNf, NaNd));
    EXPECT_TRUE(CompareRecursive(NaNd, NaNf));

    // 1.0 is arbitrary here
    EXPECT_FALSE(CompareRecursive(NaNf, 1.0));
    EXPECT_FALSE(CompareRecursive(NaNf, 1.0));
    EXPECT_FALSE(CompareRecursive(1.0, NaNd));
    EXPECT_FALSE(CompareRecursive(1.0, NaNd));
}

TEST(CompareRecursive, Optional) {
    EXPECT_TRUE(CompareRecursive(1, std::optional{1}));
    EXPECT_TRUE(CompareRecursive(std::optional{1}, 1));
    EXPECT_TRUE(CompareRecursive(std::optional{1}, std::optional{1}));

    EXPECT_FALSE(CompareRecursive(2, std::optional{1}));
    EXPECT_FALSE(CompareRecursive(std::optional{1}, 2));
    EXPECT_FALSE(CompareRecursive(std::optional{2}, std::optional{1}));
    EXPECT_FALSE(CompareRecursive(std::optional{1}, std::optional{2}));
}

TEST(CompareRecursive, OptionalNan) {
    // Special case for optional comparison:
    // NaNs should be considered as equal (compare by unpacking value of optional)

    const auto NaNf = std::numeric_limits<float>::quiet_NaN();
    const auto NaNd = std::numeric_limits<double>::quiet_NaN();

    const auto NaNfo = std::optional{NaNf};
    const auto NaNdo = std::optional{NaNd};

    EXPECT_TRUE(CompareRecursive(NaNf, NaNf));
    EXPECT_TRUE(CompareRecursive(NaNf, NaNfo));
    EXPECT_TRUE(CompareRecursive(NaNfo, NaNf));
    EXPECT_TRUE(CompareRecursive(NaNfo, NaNfo));

    EXPECT_TRUE(CompareRecursive(NaNd, NaNd));
    EXPECT_TRUE(CompareRecursive(NaNd, NaNdo));
    EXPECT_TRUE(CompareRecursive(NaNdo, NaNd));
    EXPECT_TRUE(CompareRecursive(NaNdo, NaNdo));

    EXPECT_FALSE(CompareRecursive(NaNdo, std::optional<double>{}));
    EXPECT_FALSE(CompareRecursive(NaNfo, std::optional<float>{}));
    EXPECT_FALSE(CompareRecursive(std::optional<double>{}, NaNdo));
    EXPECT_FALSE(CompareRecursive(std::optional<float>{}, NaNfo));

    // Too lazy to comparison code against std::nullopt, but this shouldn't be a problem in real life
    // following will produce compile time error:
//    EXPECT_FALSE(CompareRecursive(NaNdo, std::nullopt));
//    EXPECT_FALSE(CompareRecursive(NaNfo, std::nullopt));
}


TEST(Generators, MakeArrays) {
    auto arrays = MakeArrays<std::string, MakeStrings>();
    ASSERT_LT(0u, arrays.size());
}



TEST(PrettyPrintBlockTest, IntType) {
    Block block;

    auto i128 = std::make_shared<ColumnInt128>();
    auto i256 = std::make_shared<ColumnInt256>();

    i128->Append(std::numeric_limits<Int128>::max() / 2);
    i128->Append(std::numeric_limits<Int128>::min() / 2);
    i128->Append(Int128{0});
    i256->Append(std::numeric_limits<Int256>::max() / 2);
    i256->Append(std::numeric_limits<Int256>::min() / 2);
    i256->Append(Int128{0});
    block.AppendColumn("int128", i128);
    block.AppendColumn("int256", i256);

    std::string expected_res = R"(
+-----------------------------------------+--------------------------------------------------------------------------------+
|                                  int128 |                                                                         int256 |
+-----------------------------------------+--------------------------------------------------------------------------------+
|                                  int128 |                                                                         int256 |
+-----------------------------------------+--------------------------------------------------------------------------------+
|  85070591730234615865843651857942052863 |  28948022309329048855892746252171976963317496166410141009864396001978282409983 |
| -85070591730234615865843651857942052864 | -28948022309329048855892746252171976963317496166410141009864396001978282409984 |
|                                       0 |                                                                              0 |
+-----------------------------------------+--------------------------------------------------------------------------------+
)";
    std::stringstream sstr;
    sstr << '\n' << PrettyPrintBlock{block};
    EXPECT_EQ(expected_res, sstr.str());
}

TEST(PrettyPrintBlockTest, StringType) {
    Block block;

    auto str = std::make_shared<ColumnString>();
    auto fixed_str = std::make_shared<ColumnFixedString>(10);

    str->Append(
        "Timeplus is a streaming-first data analytics platform. It provides powerful end-to-end capabilities to help teams process "
        "streaming and historical data quickly and intuitively, accessible for organizations of all sizes and industries.");
    str->Append("'./,[]{}()*&^%$#%$#@!?<>\":");

    fixed_str->Append("stream sql");
    fixed_str->Append("Timeplus");

    block.AppendColumn("timeplus", str);
    block.AppendColumn("fixd_string", fixed_str);

    std::string expected_res = R"(
+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+
|                                                                                                                                                                                                                                   timeplus |      fixd_string |
+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+
|                                                                                                                                                                                                                                     string | fixed_string(10) |
+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+
| Timeplus is a streaming-first data analytics platform. It provides powerful end-to-end capabilities to help teams process streaming and historical data quickly and intuitively, accessible for organizations of all sizes and industries. |       stream sql |
|                                                                                                                                                                                                                 './,[]{}()*&^%$#%$#@!?<>": |         Timeplus |
+--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+------------------+
)";
    std::stringstream sstr;
    sstr << '\n' << PrettyPrintBlock{block};
    EXPECT_EQ(expected_res, sstr.str());
}

TEST(PrettyPrintBlockTest, DecimalType) {
    Block block;

    auto d1 = std::make_shared<ColumnDecimal>(9, 4);
    auto d2 = std::make_shared<ColumnDecimal>(18, 9);
    auto d3 = std::make_shared<ColumnDecimal>(9, 4);
    auto d4 = std::make_shared<ColumnDecimal>(18, 9);
    auto d5 = std::make_shared<ColumnDecimal>(38, 19);

    d1->Append(123456789);
    d2->Append(123456789012345678);
    d3->Append(123456789);
    d4->Append(123456789012345678);
    d5->Append(1234567890123456789);

    d1->Append(std::string("12345.6789"));
    d2->Append(std::string("123456789.012345678"));
    d3->Append(std::string("12345.6789"));
    d4->Append(std::string("123456789.012345678"));
    d5->Append(std::string("1234567890123456789.0123456789012345678"));

    block.AppendColumn("d1", d1);
    block.AppendColumn("d2", d2);
    block.AppendColumn("d4", d3);
    block.AppendColumn("d5", d4);
    block.AppendColumn("d6", d5);

    std::string expected_res = R"(
+--------------+--------------------+--------------+--------------------+----------------------------------------+
|           d1 |                 d2 |           d4 |                 d5 |                                     d6 |
+--------------+--------------------+--------------+--------------------+----------------------------------------+
| decimal(9,4) |      decimal(18,9) | decimal(9,4) |      decimal(18,9) |                         decimal(38,19) |
+--------------+--------------------+--------------+--------------------+----------------------------------------+
|    123456789 | 123456789012345678 |    123456789 | 123456789012345678 |                    1234567890123456789 |
|    123456789 | 123456789012345678 |    123456789 | 123456789012345678 | 12345678901234567890123456789012345678 |
+--------------+--------------------+--------------+--------------------+----------------------------------------+
)";

    std::stringstream sstr;
    sstr << '\n' << PrettyPrintBlock{block};
    EXPECT_EQ(expected_res, sstr.str());
}

TEST(PrettyPrintBlockTest, NullableTypeTest) {
    Block block;

    // The first elements of all examples are not null and second elements are null.
    auto nulls = std::make_shared<ColumnUInt8>();
    nulls->Append(0);
    nulls->Append(1);

    auto id = std::make_shared<ColumnUInt64>();
    id->Append(1);
    id->Append(2);

    auto i256 = std::make_shared<ColumnInt256>();
    i256->Append(1);
    i256->Append(2);

    auto deci = std::make_shared<ColumnDecimal>(9, 4);
    deci->Append(123.456);
    deci->Append(789.012);

    auto fl = std::make_shared<ColumnFloat32>();
    fl->Append(123.456);
    fl->Append(789.012);

    auto ip = std::make_shared<ColumnIPv4>();
    ip->Append("127.0.0.1");
    ip->Append(11111111);

    auto e1 = std::make_shared<ColumnEnum8>(Type::CreateEnum8({{"a", 1}, {"b", 2}}));
    e1->Append(1);
    e1->Append("b");

    auto st = std::make_shared<ColumnString>();
    st->Append("123.456");
    st->Append("789.012");

    auto fst = std::make_shared<ColumnFixedString>(4);
    fst->Append("1234");
    fst->Append("5678");

    auto uu = std::make_shared<ColumnUUID>();
    uu->Append(123456);
    uu->Append(789012);

    block.AppendColumn("id", std::make_shared<ColumnNullable>(id, nulls));
    block.AppendColumn("i256", std::make_shared<ColumnNullable>(i256, nulls));
    block.AppendColumn("deci", std::make_shared<ColumnNullable>(deci, nulls));
    block.AppendColumn("fl", std::make_shared<ColumnNullable>(fl, nulls));
    block.AppendColumn("ip", std::make_shared<ColumnNullable>(ip, nulls));
    block.AppendColumn("e1", std::make_shared<ColumnNullable>(e1, nulls));
    block.AppendColumn("st", std::make_shared<ColumnNullable>(st, nulls));
    block.AppendColumn("fst", std::make_shared<ColumnNullable>(fst, nulls));
    block.AppendColumn("uu", std::make_shared<ColumnNullable>(uu, nulls));

    std::string expected_res = R"(
+------------------+------------------+------------------------+-------------------+----------------+-----------------------------------+------------------+---------------------------+--------------------------------------+
|               id |             i256 |                   deci |                fl |             ip |                                e1 |               st |                       fst |                                   uu |
+------------------+------------------+------------------------+-------------------+----------------+-----------------------------------+------------------+---------------------------+--------------------------------------+
| nullable(uint64) | nullable(int256) | nullable(decimal(9,4)) | nullable(float32) | nullable(ipv4) | nullable(enum8('a' = 1, 'b' = 2)) | nullable(string) | nullable(fixed_string(4)) |                       nullable(uuid) |
+------------------+------------------+------------------------+-------------------+----------------+-----------------------------------+------------------+---------------------------+--------------------------------------+
|                1 |                1 |                    123 |           123.456 |      127.0.0.1 |                             a (1) |          123.456 |                      1234 | 00000000-0001-e240-0000-000000000000 |
|             null |             null |                   null |              null |           null |                              null |             null |                      null |                                 null |
+------------------+------------------+------------------------+-------------------+----------------+-----------------------------------+------------------+---------------------------+--------------------------------------+
)";

    std::stringstream sstr;
    sstr << '\n' << PrettyPrintBlock{block};
    EXPECT_EQ(expected_res, sstr.str());
}

TEST(PrettyPrintBlockTest, ArrayType) {
    Block block;

    auto arr_int128 = std::make_shared<ColumnArray>(std::make_shared<ColumnInt128>());
    auto nested_id = std::make_shared<ColumnArray>(std::make_shared<ColumnUInt64>());
    auto nested_f = std::make_shared<ColumnArray>(std::make_shared<ColumnFloat32>());
    auto i128 = std::make_shared<ColumnInt128>();
    i128->Append(1);
    i128->Append(2);
    i128->Append(3);
    i128->Append(4);
    arr_int128->AppendAsColumn(i128);
    arr_int128->AppendAsColumn(i128);

    // nested type
    std::vector<uint64_t> id_values1{1, 2};
    std::vector<uint64_t> id_values2{3, 4};
    nested_id->As<ColumnArray>()->AppendAsColumn(std::make_shared<ColumnUInt64>(id_values1));
    nested_id->As<ColumnArray>()->AppendAsColumn(std::make_shared<ColumnUInt64>(id_values2));

    std::vector<float> f_values1{1.1, 2.2};
    std::vector<float> f_values2{4.1213, 4.24326};
    nested_f->As<ColumnArray>()->AppendAsColumn(std::make_shared<ColumnFloat32>(f_values1));
    nested_f->As<ColumnArray>()->AppendAsColumn(std::make_shared<ColumnFloat32>(f_values2));

    block.AppendColumn("arr_int128", arr_int128);
    block.AppendColumn("n.id", nested_id);
    block.AppendColumn("n.f", nested_f);

    std::string expected_res = R"(
+---------------+---------------+-------------------+
|    arr_int128 |          n.id |               n.f |
+---------------+---------------+-------------------+
| array(int128) | array(uint64) |    array(float32) |
+---------------+---------------+-------------------+
|  [1, 2, 3, 4] |        [1, 2] |        [1.1, 2.2] |
|  [1, 2, 3, 4] |        [3, 4] | [4.1213, 4.24326] |
+---------------+---------------+-------------------+
)";

    std::stringstream sstr;
    sstr << '\n' << PrettyPrintBlock{block};
    EXPECT_EQ(expected_res, sstr.str());
}

TEST(PrettyPrintBlockTest, MultitesArrayType) {
    Block block;
    std::cout << PrettyPrintBlock{block} << '\n';

    auto m1 = std::make_shared<ColumnMapT<ColumnLowCardinalityT<ColumnFixedString>, ColumnArrayT<ColumnNullableT<ColumnUInt64>>>>(
        std::make_shared<ColumnLowCardinalityT<ColumnFixedString>>(10), std::make_shared<ColumnArrayT<ColumnNullableT<ColumnUInt64>>>());

    std::map<std::string, std::vector<std::optional<uint64_t>>> val1;
    val1["key1"] = {1, 2, std::nullopt, 4};
    val1["key2"] = {std::nullopt, 6, 7, 8};
    m1->Append(val1);

    auto m2 = std::make_shared<ColumnMapT<ColumnInt64, ColumnArrayT<ColumnNullableT<ColumnString>>>>(
        std::make_shared<ColumnInt64>(), std::make_shared<ColumnArrayT<ColumnNullableT<ColumnString>>>());
    std::map<long long, std::vector<std::optional<std::string>>> val2;
    val2[(1LL << 36)] = {"timeplus", "sql", std::nullopt};
    val2[(1LL << 62) - 1LL] = {"stream", std::nullopt, std::nullopt};
    m2->Append(val2);

    block.AppendColumn("m2", m2);
    block.AppendColumn("m1", m1);

    std::string expected_res = R"(
+-------------------------------------------------------------------------------------+-----------------------------------------------------------------+
|                                                                                  m2 |                                                              m1 |
+-------------------------------------------------------------------------------------+-----------------------------------------------------------------+
|                                                 map(int64, array(nullable(string))) | map(low_cardinality(fixed_string(10)), array(nullable(uint64))) |
+-------------------------------------------------------------------------------------+-----------------------------------------------------------------+
| {(68719476736, [timeplus, sql, null]), (4611686018427387903, [stream, null, null])} |              {(key1, [1, 2, null, 4]), (key2, [null, 6, 7, 8])} |
+-------------------------------------------------------------------------------------+-----------------------------------------------------------------+
)";

    std::stringstream sstr;
    sstr << '\n' << PrettyPrintBlock{block};
    EXPECT_EQ(expected_res, sstr.str());
}

TEST(PrettyPrintBlockTest, FloatType) {
    Block block;

    auto f32 = std::make_shared<ColumnFloat32>();
    f32->Append(std::acos(-1));
    f32->Append(std::sqrt(2));

    auto f64 = std::make_shared<ColumnFloat64>();
    f64->Append(std::acos(-1));
    f64->Append(std::sqrt(2));

    block.AppendColumn("f32", f32);
    block.AppendColumn("f64", f64);

    std::string expected_res = R"(
+---------+---------+
|     f32 |     f64 |
+---------+---------+
| float32 | float64 |
+---------+---------+
| 3.14159 | 3.14159 |
| 1.41421 | 1.41421 |
+---------+---------+
)";

    std::stringstream sstr;
    sstr << '\n' << PrettyPrintBlock{block};
    EXPECT_EQ(expected_res, sstr.str());
}

TEST(PrettyPrintBlockTest, IPType) {
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

    std::string expected_res = R"(
+--------+----------------+---------------------------+
|     id |             v4 |                        v6 |
+--------+----------------+---------------------------+
| uint64 |           ipv4 |                      ipv6 |
+--------+----------------+---------------------------+
|      1 |      127.0.0.1 |                       ::1 |
|      2 | 62.204.180.213 |                    aa::ff |
|      3 |        0.0.0.0 | fe80::86ba:ef31:f2d8:7e8b |
+--------+----------------+---------------------------+
)";

    std::stringstream sstr;
    sstr << '\n' << PrettyPrintBlock{block};
    EXPECT_EQ(expected_res, sstr.str());
}

TEST(PrettyPrintBlockTest, LowCardinalityStringType) {
    Block block;

    auto str = std::make_shared<ColumnLowCardinalityT<ColumnString>>();
    auto fixed_str = std::make_shared<ColumnLowCardinalityT<ColumnFixedString>>(10);

    str->Append("timeplus");
    str->Append("timeplus");
    str->Append("timeplus");
    str->Append("It enables data engineers and platform engineers to unlock streaming data value using SQL.");

    fixed_str->Append("stream sql");
    fixed_str->Append("1234567890");
    fixed_str->Append("stream");
    fixed_str->Append("bustub");

    block.AppendColumn("str", str);
    block.AppendColumn("fixed_str", fixed_str);

    std::string expected_res = R"(
+--------------------------------------------------------------------------------------------+-----------------------------------+
|                                                                                        str |                         fixed_str |
+--------------------------------------------------------------------------------------------+-----------------------------------+
|                                                                    low_cardinality(string) | low_cardinality(fixed_string(10)) |
+--------------------------------------------------------------------------------------------+-----------------------------------+
|                                                                                   timeplus |                        stream sql |
|                                                                                   timeplus |                        1234567890 |
|                                                                                   timeplus |                            stream |
| It enables data engineers and platform engineers to unlock streaming data value using SQL. |                            bustub |
+--------------------------------------------------------------------------------------------+-----------------------------------+
)";

    std::stringstream sstr;
    sstr << '\n' << PrettyPrintBlock{block};
    EXPECT_EQ(expected_res, sstr.str());
}

TEST(PrettyPrintBlockTest, TupleType) {
    Block block;

    auto id = std::make_shared<ColumnUInt64>();
    id->Append(1);
    id->Append(2);
    id->Append(3);

    auto tupls = std::make_shared<ColumnTuple>(std::vector<ColumnRef>{std::make_shared<ColumnUInt64>(), std::make_shared<ColumnString>()});

    (*tupls)[0]->As<ColumnUInt64>()->Append(369);
    (*tupls)[1]->As<ColumnString>()->Append("stream sql");

    (*tupls)[0]->As<ColumnUInt64>()->Append(957);
    (*tupls)[1]->As<ColumnString>()->Append("Timeplus");

    (*tupls)[0]->As<ColumnUInt64>()->Append(114514);
    (*tupls)[1]->As<ColumnString>()->Append("db");

    block.AppendColumn("id", id);
    block.AppendColumn("tup", tupls);

    std::string expected_res = R"(
+--------+-----------------------+
|     id |                   tup |
+--------+-----------------------+
| uint64 | tuple(uint64, string) |
+--------+-----------------------+
|      1 |     (369, stream sql) |
|      2 |       (957, Timeplus) |
|      3 |          (114514, db) |
+--------+-----------------------+
)";

    std::stringstream sstr;
    sstr << '\n' << PrettyPrintBlock{block};
    EXPECT_EQ(expected_res, sstr.str());
}

