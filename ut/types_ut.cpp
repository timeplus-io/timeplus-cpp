#include <timeplus/types/types.h>
#include <timeplus/columns/factory.h>
#include <ut/utils.h>

#include <gtest/gtest.h>

using namespace timeplus;

TEST(TypesCase, TypeName) {
    ASSERT_EQ(Type::CreateDate()->GetName(), "date");

    ASSERT_EQ(Type::CreateArray(Type::CreateSimple<int32_t>())->GetName(), "array(int32)");

    ASSERT_EQ(Type::CreateNullable(Type::CreateSimple<int32_t>())->GetName(), "nullable(int32)");

    ASSERT_EQ(Type::CreateArray(Type::CreateSimple<int32_t>())->As<ArrayType>()->GetItemType()->GetCode(), Type::Int32);

    ASSERT_EQ(Type::CreateTuple({Type::CreateSimple<int32_t>(), Type::CreateString()})->GetName(), "tuple(int32, string)");

    ASSERT_EQ(
        Type::CreateTuple({
            Type::CreateSimple<int32_t>(),
            Type::CreateString()})->GetName(),
        "tuple(int32, string)"
    );

    ASSERT_EQ(
        Type::CreateEnum8({{"One", 1}})->GetName(),
        "enum8('One' = 1)"
    );
    ASSERT_EQ(
        Type::CreateEnum8({})->GetName(),
        "enum8()"
    );

    ASSERT_EQ(Type::CreateMap(Type::CreateSimple<int32_t>(), Type::CreateString())->GetName(), "map(int32, string)");
}

TEST(TypesCase, NullableType) {
    TypeRef nested = Type::CreateSimple<int32_t>();
    ASSERT_EQ(Type::CreateNullable(nested)->As<NullableType>()->GetNestedType(), nested);
}

TEST(TypesCase, EnumTypes) {
    auto enum8 = Type::CreateEnum8({{"One", 1}, {"Two", 2}});
    ASSERT_EQ(enum8->GetName(), "enum8('One' = 1, 'Two' = 2)");
    ASSERT_TRUE(enum8->As<EnumType>()->HasEnumValue(1));
    ASSERT_TRUE(enum8->As<EnumType>()->HasEnumName("Two"));
    ASSERT_FALSE(enum8->As<EnumType>()->HasEnumValue(10));
    ASSERT_FALSE(enum8->As<EnumType>()->HasEnumName("Ten"));
    ASSERT_EQ(enum8->As<EnumType>()->GetEnumName(2), "Two");
    ASSERT_EQ(enum8->As<EnumType>()->GetEnumValue("Two"), 2);

    auto enum16 = Type::CreateEnum16({{"Green", 1}, {"Red", 2}, {"Yellow", 3}});
    ASSERT_EQ(enum16->GetName(), "enum16('Green' = 1, 'Red' = 2, 'Yellow' = 3)");
    ASSERT_TRUE(enum16->As<EnumType>()->HasEnumValue(3));
    ASSERT_TRUE(enum16->As<EnumType>()->HasEnumName("Green"));
    ASSERT_FALSE(enum16->As<EnumType>()->HasEnumValue(10));
    ASSERT_FALSE(enum16->As<EnumType>()->HasEnumName("Black"));
    ASSERT_EQ(enum16->As<EnumType>()->GetEnumName(2), "Red");
    ASSERT_EQ(enum16->As<EnumType>()->GetEnumValue("Green"), 1);

    ASSERT_EQ(std::distance(enum16->As<EnumType>()->BeginValueToName(), enum16->As<EnumType>()->EndValueToName()), 3u);
    ASSERT_EQ(enum16->As<EnumType>()->BeginValueToName()->first, 1);
    ASSERT_EQ(enum16->As<EnumType>()->BeginValueToName()->second, "Green");
    ASSERT_EQ((++enum16->As<EnumType>()->BeginValueToName())->first, 2);
    ASSERT_EQ((++enum16->As<EnumType>()->BeginValueToName())->second, "Red");
}

TEST(TypesCase, EnumTypesEmpty) {
    ASSERT_EQ("enum8()", Type::CreateEnum8({})->GetName());
    ASSERT_EQ("enum16()", Type::CreateEnum16({})->GetName());
}

TEST(TypesCase, DecimalTypes) {
    // TODO: implement this test.
}

TEST(TypesCase, IsEqual) {
    const std::string type_names[] = {
        "uint8",
        "int8",
        "uint128",
        "string",
        "fixed_string(0)",
        "fixed_string(10000)",
        "datetime('UTC')",
        "datetime64(3, 'UTC')",
        "decimal(9,3)",
        "decimal(18,3)",
        "enum8('ONE' = 1)",
        "enum8('ONE' = 1, 'TWO' = 2)",
        "enum16('ONE' = 1, 'TWO' = 2, 'THREE' = 3, 'FOUR' = 4)",
        "nullable(fixed_string(10000))",
        "nullable(low_cardinality(fixed_string(10000)))",
        "array(int8)",
        "array(uint8)",
        "array(string)",
        "array(nullable(low_cardinality(fixed_string(10000))))",
        "array(enum8('ONE' = 1, 'TWO' = 2))"
        "tuple(string, int8, date, datetime)",
        "nullable(tuple(string, int8, date, datetime))",
        "array(nullable(tuple(string, int8, date, datetime)))",
        "array(array(nullable(tuple(string, int8, date, datetime))))",
        "array(array(array(nullable(tuple(string, int8, date, datetime)))))",
        "array(array(array(array(nullable(tuple(string, int8, date, datetime('UTC')))))))"
        "array(array(array(array(nullable(tuple(string, int8, date, datetime('UTC'), tuple(low_cardinality(String), enum8('READ'=1, 'WRITE'=0))))))))",
        "map(string, int8)",
        "map(string, tuple(string, int8, date, datetime))",
        "map(uuid, array(tuple(string, int8, date, datetime)))",
        "map(string, array(array(array(nullable(tuple(string, int8, date, datetime))))))",
        "map(low_cardinality(fixed_string(10)), array(array(array(array(nullable(tuple(string, int8, date, datetime('UTC'))))))))",
        "point",
        "ring",
        "polygon",
        "multi_polygon"
    };

    // Check that Type::IsEqual returns true only if:
    // - same Type instance
    // - same Type layout (matching outer type with all nested types and/or parameters)
    for (const auto & type_name : type_names) {
        SCOPED_TRACE(type_name);
        const auto type = timeplus::CreateColumnByType(type_name)->Type();

        // Should be equal to itself
        EXPECT_TRUE(type->IsEqual(type));
        EXPECT_TRUE(type->IsEqual(*type));

        for (const auto & other_type_name : type_names) {
            SCOPED_TRACE(other_type_name);
            const auto other_column = timeplus::CreateColumnByType(other_type_name);
            ASSERT_NE(nullptr, other_column);

            const auto other_type = other_column->Type();

            const auto should_be_equal = type_name == other_type_name;
            EXPECT_EQ(should_be_equal, type->IsEqual(other_type))
                        << "For types: " << type_name << " and " << other_type_name;
        }
    }
}

TEST(TypesCase, ErrorEnumContent) {
    const std::string type_names[] = {
        "enum8()",
        "enum8('ONE')",
        "enum8('ONE'=1,'TWO')",
        "enum16('TWO'=,'TWO')",
    };

    for (const auto& type_name : type_names) {
        SCOPED_TRACE(type_name);
        EXPECT_THROW(timeplus::CreateColumnByType(type_name)->Type(), ValidationError);
    }
}
