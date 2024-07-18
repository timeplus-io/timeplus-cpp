#include <clickhouse/columns/factory.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>

#include <gtest/gtest.h>

namespace {
using namespace clickhouse;
}

TEST(CreateColumnByType, CreateSimpleAggregateFunction) {
    auto col = CreateColumnByType("simple_aggregate_function(funt, int32)");

    ASSERT_EQ("int32", col->Type()->GetName());
    ASSERT_EQ(Type::Int32, col->Type()->GetCode());
    ASSERT_NE(nullptr, col->As<ColumnInt32>());
}

TEST(CreateColumnByType, UnmatchedBrackets) {
    // When type string has unmatched brackets, CreateColumnByType must return nullptr.
    ASSERT_EQ(nullptr, CreateColumnByType("fixed_string(10"));
    ASSERT_EQ(nullptr, CreateColumnByType("nullable(fixed_string(10000"));
    ASSERT_EQ(nullptr, CreateColumnByType("nullable(fixed_string(10000)"));
    ASSERT_EQ(nullptr, CreateColumnByType("low_cardinality(nullable(fixed_string(10000"));
    ASSERT_EQ(nullptr, CreateColumnByType("low_cardinality(nullable(fixed_string(10000)"));
    ASSERT_EQ(nullptr, CreateColumnByType("low_cardinality(nullable(fixed_string(10000))"));
    ASSERT_EQ(nullptr, CreateColumnByType("array(low_cardinality(nullable(fixed_string(10000"));
    ASSERT_EQ(nullptr, CreateColumnByType("array(low_cardinality(nullable(fixed_string(10000)"));
    ASSERT_EQ(nullptr, CreateColumnByType("array(low_cardinality(nullable(fixed_string(10000))"));
    ASSERT_EQ(nullptr, CreateColumnByType("array(low_cardinality(nullable(fixed_string(10000)))"));
}

TEST(CreateColumnByType, LowCardinalityAsWrappedColumn) {
    CreateColumnByTypeSettings create_column_settings;
    create_column_settings.low_cardinality_as_wrapped_column = true;

    ASSERT_EQ(Type::String, CreateColumnByType("low_cardinality(string)", create_column_settings)->GetType().GetCode());
    ASSERT_EQ(Type::String, CreateColumnByType("low_cardinality(string)", create_column_settings)->As<ColumnString>()->GetType().GetCode());

    ASSERT_EQ(Type::FixedString, CreateColumnByType("low_cardinality(fixed_string(10000))", create_column_settings)->GetType().GetCode());
    ASSERT_EQ(Type::FixedString, CreateColumnByType("low_cardinality(fixed_string(10000))", create_column_settings)->As<ColumnFixedString>()->GetType().GetCode());
}

TEST(CreateColumnByType, DateTime) {
    ASSERT_NE(nullptr, CreateColumnByType("datetime"));
    ASSERT_NE(nullptr, CreateColumnByType("datetime('Europe/Moscow')"));

    ASSERT_EQ(CreateColumnByType("datetime('UTC')")->As<ColumnDateTime>()->Timezone(), "UTC");
    ASSERT_EQ(CreateColumnByType("datetime64(3, 'UTC')")->As<ColumnDateTime64>()->Timezone(), "UTC");
}

TEST(CreateColumnByType, AggregateFunction) {
    EXPECT_EQ(nullptr, CreateColumnByType("aggregate_function(argMax, int32, datetime64(3))"));
    EXPECT_EQ(nullptr, CreateColumnByType("aggregate_function(argMax, fixed_string(10), datetime64(3, 'UTC'))"));
}


class CreateColumnByTypeWithName : public ::testing::TestWithParam<const char* /*Column Type String*/>
{};

TEST(CreateColumnByType, Bool) {
    const auto col = CreateColumnByType("bool");
    ASSERT_NE(nullptr, col);
    EXPECT_EQ(col->GetType().GetName(), "uint8");
}

TEST_P(CreateColumnByTypeWithName, CreateColumnByType)
{
    const auto col = CreateColumnByType(GetParam());
    ASSERT_NE(nullptr, col);
    EXPECT_EQ(col->GetType().GetName(), GetParam());
}

INSTANTIATE_TEST_SUITE_P(Basic, CreateColumnByTypeWithName, ::testing::Values(
    "int8", "int16", "int32", "int64",
    "uint8", "uint16", "uint32", "uint64",
    "string", "date", "datetime",
    "uuid", "int128"
));

INSTANTIATE_TEST_SUITE_P(Parametrized, CreateColumnByTypeWithName, ::testing::Values(
    "fixed_string(0)", "fixed_string(10000)",
    "datetime('UTC')", "datetime64(3, 'UTC')",
    "decimal(9,3)", "decimal(18,3)",
    "enum8('ONE' = 1, 'TWO' = 2)",
    "enum16('ONE' = 1, 'TWO' = 2, 'THREE' = 3, 'FOUR' = 4)"
));


INSTANTIATE_TEST_SUITE_P(Nested, CreateColumnByTypeWithName, ::testing::Values(
    "nullable(fixed_string(10000))",
    "nullable(low_cardinality(fixed_string(10000)))",
    "array(nullable(low_cardinality(fixed_string(10000))))",
    "array(enum8('ONE' = 1, 'TWO' = 2))"
));
