#include <clickhouse/types/type_parser.h>
#include <gtest/gtest.h>

using namespace timeplus;

// TODO: add tests for Decimal column types.

TEST(TypeParserCase, ParseTerminals) {
    TypeAst ast;
    TypeParser("uint8").Parse(&ast);

    ASSERT_EQ(ast.meta, TypeAst::Terminal);
    ASSERT_EQ(ast.name, "uint8");
    ASSERT_EQ(ast.code, Type::UInt8);
}

TEST(TypeParserCase, ParseFixedString) {
    TypeAst ast;
    TypeParser("fixed_string(24)").Parse(&ast);

    ASSERT_EQ(ast.meta, TypeAst::Terminal);
    ASSERT_EQ(ast.name, "fixed_string");
    ASSERT_EQ(ast.code, Type::FixedString);
    ASSERT_EQ(ast.elements.front().value, 24U);
}

TEST(TypeParserCase, ParseArray) {
    TypeAst ast;
    TypeParser("array(int32)").Parse(&ast);

    ASSERT_EQ(ast.meta, TypeAst::Array);
    ASSERT_EQ(ast.name, "array");
    ASSERT_EQ(ast.code, Type::Array);
    ASSERT_EQ(ast.elements.front().meta, TypeAst::Terminal);
    ASSERT_EQ(ast.elements.front().name, "int32");
}

TEST(TypeParserCase, ParseNullable) {
    TypeAst ast;
    TypeParser("nullable(date)").Parse(&ast);

    ASSERT_EQ(ast.meta, TypeAst::Nullable);
    ASSERT_EQ(ast.name, "nullable");
    ASSERT_EQ(ast.code, Type::Nullable);
    ASSERT_EQ(ast.elements.front().meta, TypeAst::Terminal);
    ASSERT_EQ(ast.elements.front().name, "date");
}

TEST(TypeParserCase, ParseEnum) {
    TypeAst ast;
    TypeParser(
        "enum8('COLOR_red_10_T' = -12, 'COLOR_green_20_T'=-25, 'COLOR_blue_30_T'= 53, 'COLOR_black_30_T' = 107")
        .Parse(&ast);
    ASSERT_EQ(ast.meta, TypeAst::Enum);
    ASSERT_EQ(ast.name, "enum8");
    ASSERT_EQ(ast.code, Type::Enum8);
    ASSERT_EQ(ast.elements.size(), 8u);

    std::vector<std::string> names = {"COLOR_red_10_T", "COLOR_green_20_T", "COLOR_blue_30_T", "COLOR_black_30_T"};
    std::vector<int16_t> values = {-12, -25, 53, 107};

    auto element = ast.elements.begin();
    for (size_t i = 0; i < 4; ++i) {
        EXPECT_EQ(element->code, Type::String);
        EXPECT_EQ(element->meta, TypeAst::Terminal);
        EXPECT_EQ(element->value_string, names[i]);

        ++element;
        EXPECT_EQ(element->code, Type::Void);
        EXPECT_EQ(element->meta, TypeAst::Number);
        EXPECT_EQ(element->value, values[i]);

        ++element;
    }
}

TEST(TypeParserCase, ParseTuple) {
    TypeAst ast;
    TypeParser(
        "tuple(uint8, string)")
        .Parse(&ast);
    ASSERT_EQ(ast.meta, TypeAst::Tuple);
    ASSERT_EQ(ast.name, "tuple");
    ASSERT_EQ(ast.code, Type::Tuple);
    ASSERT_EQ(ast.elements.size(), 2u);

    std::vector<std::string> names = {"uint8", "string"};

    auto element = ast.elements.begin();
    for (size_t i = 0; i < 2; ++i) {
        ASSERT_EQ(element->name, names[i]);
        ++element;
    }
}

TEST(TypeParserCase, ParseDecimal) {
    TypeAst ast;
    TypeParser("decimal(12, 5)").Parse(&ast);
    ASSERT_EQ(ast.meta, TypeAst::Terminal);
    ASSERT_EQ(ast.name, "decimal");
    ASSERT_EQ(ast.code, Type::Decimal);
    ASSERT_EQ(ast.elements.size(), 2u);
    ASSERT_EQ(ast.elements[0].value, 12);
    ASSERT_EQ(ast.elements[1].value, 5);
}

TEST(TypeParserCase, ParseDecimal32) {
    TypeAst ast;
    TypeParser("decimal32(7)").Parse(&ast);
    ASSERT_EQ(ast.meta, TypeAst::Terminal);
    ASSERT_EQ(ast.name, "decimal32");
    ASSERT_EQ(ast.code, Type::Decimal32);
    ASSERT_EQ(ast.elements.size(), 1u);
    ASSERT_EQ(ast.elements[0].value, 7);
}

TEST(TypeParserCase, ParseDecimal64) {
    TypeAst ast;
    TypeParser("decimal64(1)").Parse(&ast);
    ASSERT_EQ(ast.meta, TypeAst::Terminal);
    ASSERT_EQ(ast.name, "decimal64");
    ASSERT_EQ(ast.code, Type::Decimal64);
    ASSERT_EQ(ast.elements.size(), 1u);
    ASSERT_EQ(ast.elements[0].value, 1);
}

TEST(TypeParserCase, ParseDecimal128) {
    TypeAst ast;
    TypeParser("decimal128(3)").Parse(&ast);
    ASSERT_EQ(ast.meta, TypeAst::Terminal);
    ASSERT_EQ(ast.name, "decimal128");
    ASSERT_EQ(ast.code, Type::Decimal128);
    ASSERT_EQ(ast.elements.size(), 1u);
    ASSERT_EQ(ast.elements[0].value, 3);
}

TEST(TypeParserCase, ParseDateTime_NO_TIMEZONE) {
    TypeAst ast;
    TypeParser("datetime").Parse(&ast);
    ASSERT_EQ(ast.meta, TypeAst::Terminal);
    ASSERT_EQ(ast.name, "datetime");
    ASSERT_EQ(ast.code, Type::DateTime);
    ASSERT_EQ(ast.elements.size(), 0u);
}

TEST(TypeParserCase, ParseDateTime_UTC_TIMEZONE) {
    TypeAst ast;
    TypeParser("datetime('UTC')").Parse(&ast);
    ASSERT_EQ(ast.meta, TypeAst::Terminal);
    ASSERT_EQ(ast.name, "datetime");
    ASSERT_EQ(ast.code, Type::DateTime);
    ASSERT_EQ(ast.elements.size(), 1u);

    ASSERT_EQ(ast.elements[0].code, Type::String);
    ASSERT_EQ(ast.elements[0].value_string, "UTC");
    ASSERT_EQ(ast.elements[0].meta, TypeAst::Terminal);
}

TEST(TypeParserCase, ParseDateTime_MINSK_TIMEZONE) {
    TypeAst ast;
    TypeParser("datetime('Europe/Minsk')").Parse(&ast);
    ASSERT_EQ(ast.meta, TypeAst::Terminal);
    ASSERT_EQ(ast.name, "datetime");
    ASSERT_EQ(ast.code, Type::DateTime);
    ASSERT_EQ(ast.elements[0].code, Type::String);
    ASSERT_EQ(ast.elements[0].value_string, "Europe/Minsk");
    ASSERT_EQ(ast.elements[0].meta, TypeAst::Terminal);
}

TEST(TypeParserCase, LowCardinality_String) {
    TypeAst ast;
    ASSERT_TRUE(TypeParser("low_cardinality(string)").Parse(&ast));
    ASSERT_EQ(ast.meta, TypeAst::LowCardinality);
    ASSERT_EQ(ast.name, "low_cardinality");
    ASSERT_EQ(ast.code, Type::LowCardinality);
    ASSERT_EQ(ast.elements.size(), 1u);
    ASSERT_EQ(ast.elements[0].meta, TypeAst::Terminal);
    ASSERT_EQ(ast.elements[0].code, Type::String);
    ASSERT_EQ(ast.elements[0].name, "string");
    ASSERT_EQ(ast.elements[0].value, 0);
    ASSERT_EQ(ast.elements[0].elements.size(), 0u);
}

TEST(TypeParserCase, LowCardinality_FixedString) {
    TypeAst ast;
    ASSERT_TRUE(TypeParser("low_cardinality(fixed_string(10))").Parse(&ast));
    ASSERT_EQ(ast.meta, TypeAst::LowCardinality);
    ASSERT_EQ(ast.name, "low_cardinality");
    ASSERT_EQ(ast.code, Type::LowCardinality);
    ASSERT_EQ(ast.elements.size(), 1u);
    ASSERT_EQ(ast.elements.size(), 1u);
    ASSERT_EQ(ast.elements[0].meta, TypeAst::Terminal);
    ASSERT_EQ(ast.elements[0].code, Type::FixedString);
    ASSERT_EQ(ast.elements[0].name, "fixed_string");
    ASSERT_EQ(ast.elements[0].value, 0);
    ASSERT_EQ(ast.elements[0].elements.size(), 1u);
    auto param = TypeAst{TypeAst::Number, Type::Void, "", 10, {}, {}};
    ASSERT_EQ(ast.elements[0].elements[0], param);
}

TEST(TypeParserCase, SimpleAggregateFunction_UInt64) {
    TypeAst ast;
    TypeParser("simple_aggregate_function(func, uint64)").Parse(&ast);
    ASSERT_EQ(ast.meta, TypeAst::SimpleAggregateFunction);
    ASSERT_EQ(ast.name, "simple_aggregate_function");
    ASSERT_EQ(ast.code, Type::Void);
    ASSERT_EQ(ast.elements.size(), 2u);
    ASSERT_EQ(ast.elements[0].name, "func");
    ASSERT_EQ(ast.elements[0].code, Type::Void);
    ASSERT_EQ(ast.elements[0].meta, TypeAst::Terminal);
    ASSERT_EQ(ast.elements[0].value, 0);
    ASSERT_EQ(ast.elements[1].name, "uint64");
    ASSERT_EQ(ast.elements[1].code, Type::UInt64);
    ASSERT_EQ(ast.elements[1].meta, TypeAst::Terminal);
}

TEST(TypeParserCase, ParseDateTime64) {
    TypeAst ast;
    TypeParser("datetime64(3, 'UTC')").Parse(&ast);
    ASSERT_EQ(ast.meta, TypeAst::Terminal);
    ASSERT_EQ(ast.name, "datetime64");
    ASSERT_EQ(ast.code, Type::DateTime64);
    ASSERT_EQ(ast.elements.size(), 2u);
    ASSERT_EQ(ast.elements[0].name, "");
    ASSERT_EQ(ast.elements[0].value, 3);
    ASSERT_EQ(ast.elements[1].value_string, "UTC");
    ASSERT_EQ(ast.elements[1].value, 0);
}

TEST(TypeParserCase, ParseMap) {
    TypeAst ast;
    TypeParser("map(int32, string)").Parse(&ast);
    ASSERT_EQ(ast.meta, TypeAst::Map);
    ASSERT_EQ(ast.name, "map");
    ASSERT_EQ(ast.code, Type::Map);
    ASSERT_EQ(ast.elements.size(), 2u);
    ASSERT_EQ(ast.elements[0].meta, TypeAst::Terminal);
    ASSERT_EQ(ast.elements[0].name, "int32");
    ASSERT_EQ(ast.elements[1].meta, TypeAst::Terminal);
    ASSERT_EQ(ast.elements[1].name, "string");
}

TEST(TypeParser, EmptyName) {
    {
        TypeAst ast;
        EXPECT_EQ(false, TypeParser("").Parse(&ast));
    }

    {
        TypeAst ast;
        EXPECT_EQ(false, TypeParser(" ").Parse(&ast));
    }
}

TEST(ParseTypeName, EmptyName) {
    // Empty and invalid names shouldn't produce any AST and shoudn't crash
    EXPECT_EQ(nullptr, ParseTypeName(""));
    EXPECT_EQ(nullptr, ParseTypeName(" "));
    EXPECT_EQ(nullptr, ParseTypeName(std::string(5, '\0')));
}

TEST(TypeParser, AggregateFunction) {
    {
        TypeAst ast;
        EXPECT_FALSE(TypeParser("aggregate_function(argMax, int32, datetime(3))").Parse(&ast));
    }

    {
        TypeAst ast;
        EXPECT_FALSE(TypeParser("aggregate_function(argMax, low_cardinality(nullable(fixed_string(4))), datetime(3, 'UTC'))").Parse(&ast));
    }
}
