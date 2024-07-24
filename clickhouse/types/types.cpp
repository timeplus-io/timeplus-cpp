#include "types.h"

#include "../exceptions.h"

#include <city.h>

#include <stdexcept>

namespace clickhouse {

Type::Type(const Code code)
    : code_(code)
    , type_unique_id_(0)
{}

const char* Type::TypeName(Type::Code code) {
    switch (code) {
        case Type::Code::Void:           return "void";
        case Type::Code::Int8:           return "int8";
        case Type::Code::Int16:          return "int16";
        case Type::Code::Int32:          return "int32";
        case Type::Code::Int64:          return "int64";
        case Type::Code::UInt8:          return "uint8";
        case Type::Code::UInt16:         return "uint16";
        case Type::Code::UInt32:         return "uint32";
        case Type::Code::UInt64:         return "uint64";
        case Type::Code::Float32:        return "float32";
        case Type::Code::Float64:        return "float64";
        case Type::Code::String:         return "string";
        case Type::Code::FixedString:    return "fixed_string";
        case Type::Code::DateTime:       return "datetime";
        case Type::Code::Date:           return "date";
        case Type::Code::Array:          return "array";
        case Type::Code::Nullable:       return "nullable";
        case Type::Code::Tuple:          return "tuple";
        case Type::Code::Enum8:          return "enum8";
        case Type::Code::Enum16:         return "enum16";
        case Type::Code::UUID:           return "uuid";
        case Type::Code::IPv4:           return "ipv4";
        case Type::Code::IPv6:           return "ipv6";
        case Type::Code::Int128:         return "int128";
        case Type::Code::Decimal:        return "decimal";
        case Type::Code::Decimal32:      return "decimal32";
        case Type::Code::Decimal64:      return "decimal64";
        case Type::Code::Decimal128:     return "decimal128";
        case Type::Code::LowCardinality: return "low_cardinality";
        case Type::Code::DateTime64:     return "datetime64";
        case Type::Code::Date32:         return "date32";
        case Type::Code::Map:            return "map";
        case Type::Code::Point:          return "point";
        case Type::Code::Ring:           return "ring";
        case Type::Code::Polygon:        return "polygon";
        case Type::Code::MultiPolygon:   return "multi_polygon";
        case Type::Code::UInt128:        return "uint128";
        case Type::Code::Int256:         return "int256";
        case Type::Code::UInt256:        return "uint256";
    }

    return "Unknown type";
}

std::string Type::GetName() const {
    switch (code_) {
        case Void:
        case Int8:
        case Int16:
        case Int32:
        case Int64:
        case Int128:
        case UInt8:
        case UInt16:
        case UInt32:
        case UInt64:
        case UUID:
        case Float32:
        case Float64:
        case String:
        case IPv4:
        case IPv6:
        case Date:
        case Date32:
        case Point:
        case Ring:
        case Polygon:
        case MultiPolygon:
        case UInt128:
        case Int256:
        case UInt256:
            return TypeName(code_);
        case FixedString:
            return As<FixedStringType>()->GetName();
        case DateTime:
            return As<DateTimeType>()->GetName();
        case DateTime64:
            return As<DateTime64Type>()->GetName();
        case Array:
            return As<ArrayType>()->GetName();
        case Nullable:
            return As<NullableType>()->GetName();
        case Tuple:
            return As<TupleType>()->GetName();
        case Enum8:
        case Enum16:
            return As<EnumType>()->GetName();
        case Decimal:
        case Decimal32:
        case Decimal64:
        case Decimal128:
            return As<DecimalType>()->GetName();
        case LowCardinality:
            return As<LowCardinalityType>()->GetName();
        case Map:
            return As<MapType>()->GetName();
    }

    // XXX: NOT REACHED!
    return std::string();
}

uint64_t Type::GetTypeUniqueId() const {
    // Helper method to optimize equality checks of types with Type::IsEqual(),
    // base invariant: types with same names produce same unique id (and hence considered equal).
    // As an optimization, full type name is constructed at most once, and only for complex types.
    switch (code_) {
        case Void:
        case Int8:
        case Int16:
        case Int32:
        case Int64:
        case Int128:
        case UInt8:
        case UInt16:
        case UInt32:
        case UInt64:
        case UUID:
        case Float32:
        case Float64:
        case String:
        case IPv4:
        case IPv6:
        case Date:
        case Date32:
        case Point:
        case Ring:
        case Polygon:
        case MultiPolygon:        
        case UInt128:
        case Int256:
        case UInt256:
            // For simple types, unique ID is the same as Type::Code
            return code_;

        case FixedString:
        case DateTime:
        case DateTime64:
        case Array:
        case Nullable:
        case Tuple:
        case Enum8:
        case Enum16:
        case Decimal:
        case Decimal32:
        case Decimal64:
        case Decimal128:
        case LowCardinality:
        case Map: {
            // For complex types, exact unique ID depends on nested types and/or parameters,
            // the easiest way is to lazy-compute unique ID from name once.
            // Here we do not care if multiple threads are computing value simultaneosly since it is both:
            //   1. going to be the same
            //   2. going to be stored atomically

            if (type_unique_id_.load(std::memory_order_relaxed) == 0) {
                const auto name = GetName();
                type_unique_id_.store(CityHash64WithSeed(name.c_str(), name.size(), code_), std::memory_order_relaxed);
            }

            return type_unique_id_;
        }
    }
    assert(false);
    return 0;
}

TypeRef Type::CreateArray(TypeRef item_type) {
    return TypeRef(new ArrayType(item_type));
}

TypeRef Type::CreateDate() {
    return TypeRef(new Type(Type::Date));
}

TypeRef Type::CreateDate32() {
    return TypeRef(new Type(Type::Date32));
}

TypeRef Type::CreateDateTime(std::string timezone) {
    return TypeRef(new DateTimeType(std::move(timezone)));
}

TypeRef Type::CreateDateTime64(size_t precision, std::string timezone) {
    return TypeRef(new DateTime64Type(precision, std::move(timezone)));
}

TypeRef Type::CreateDecimal(size_t precision, size_t scale) {
    return TypeRef(new DecimalType(precision, scale));
}

TypeRef Type::CreateIPv4() {
    return TypeRef(new Type(Type::IPv4));
}

TypeRef Type::CreateIPv6() {
    return TypeRef(new Type(Type::IPv6));
}

TypeRef Type::CreateNothing() {
    return TypeRef(new Type(Type::Void));
}

TypeRef Type::CreateNullable(TypeRef nested_type) {
    return TypeRef(new NullableType(nested_type));
}

TypeRef Type::CreateString() {
    return TypeRef(new Type(Type::String));
}

TypeRef Type::CreateString(size_t n) {
    return TypeRef(new FixedStringType(n));
}

TypeRef Type::CreateTuple(const std::vector<TypeRef>& item_types) {
    return TypeRef(new TupleType(item_types));
}

TypeRef Type::CreateEnum8(const std::vector<EnumItem>& enum_items) {
    return TypeRef(new EnumType(Type::Enum8, enum_items));
}

TypeRef Type::CreateEnum16(const std::vector<EnumItem>& enum_items) {
    return TypeRef(new EnumType(Type::Enum16, enum_items));
}

TypeRef Type::CreateUUID() {
    return TypeRef(new Type(Type::UUID));
}

TypeRef Type::CreateLowCardinality(TypeRef item_type) {
    return std::make_shared<LowCardinalityType>(item_type);
}

TypeRef Type::CreateMap(TypeRef key_type, TypeRef value_type) {
    return std::make_shared<MapType>(key_type, value_type);
}

TypeRef Type::CreatePoint() {
    return TypeRef(new Type(Type::Point));
}

TypeRef Type::CreateRing() {
    return TypeRef(new Type(Type::Ring));
}

TypeRef Type::CreatePolygon() {
    return TypeRef(new Type(Type::Polygon));
}

TypeRef Type::CreateMultiPolygon() {
    return TypeRef(new Type(Type::MultiPolygon));
}

/// class ArrayType

ArrayType::ArrayType(TypeRef item_type) : Type(Array), item_type_(item_type) {
}

/// class DecimalType

DecimalType::DecimalType(size_t precision, size_t scale)
    : Type(Decimal),
      precision_(precision),
      scale_(scale) {
    // TODO: assert(precision <= 38 && precision > 0);
}

std::string DecimalType::GetName() const {
    switch (GetCode()) {
        case Decimal:
            return "decimal(" + std::to_string(precision_) + "," + std::to_string(scale_) + ")";
        case Decimal32:
            return "decimal32(" + std::to_string(scale_) + ")";
        case Decimal64:
            return "decimal64(" + std::to_string(scale_) + ")";
        case Decimal128:
            return "decimal128(" + std::to_string(scale_) + ")";
        default:
            /// XXX: NOT REACHED!
            return "";
    }
}

/// class EnumType

EnumType::EnumType(Type::Code type, const std::vector<EnumItem>& items) : Type(type) {
    for (const auto& item : items) {
        auto result = name_to_value_.insert(item);
        value_to_name_[item.second] = result.first->first;
    }
}

std::string EnumType::GetName() const {
    std::string result;

    if (GetCode() == Enum8) {
        result = "enum8(";
    } else {
        result = "enum16(";
    }

    for (auto ei = value_to_name_.begin(); ei != value_to_name_.end();) {
        result += "'";
        result += ei->second;
        result += "' = ";
        result += std::to_string(ei->first);

        if (++ei != value_to_name_.end()) {
            result += ", ";
        } else {
            break;
        }
    }

    result += ")";

    return result;
}

std::string_view EnumType::GetEnumName(int16_t value) const {
    return value_to_name_.at(value);
}

int16_t EnumType::GetEnumValue(const std::string& name) const {
    return name_to_value_.at(name);
}

bool EnumType::HasEnumName(const std::string& name) const {
    return name_to_value_.find(name) != name_to_value_.end();
}

bool EnumType::HasEnumValue(int16_t value) const {
    return value_to_name_.find(value) != value_to_name_.end();
}

EnumType::ValueToNameIterator EnumType::BeginValueToName() const {
    return value_to_name_.begin();
}

EnumType::ValueToNameIterator EnumType::EndValueToName() const {
    return value_to_name_.end();
}


namespace details
{
TypeWithTimeZoneMixin::TypeWithTimeZoneMixin(std::string timezone)
    : timezone_(std::move(timezone)) {
}

const std::string & TypeWithTimeZoneMixin::Timezone() const {
    return timezone_;
}
}

/// class DateTimeType
DateTimeType::DateTimeType(std::string timezone)
    : Type(DateTime), details::TypeWithTimeZoneMixin(std::move(timezone)) {
}

std::string DateTimeType::GetName() const {
    std::string datetime_representation = "datetime";
    const auto & timezone = Timezone();
    if (!timezone.empty())
        datetime_representation += "('" + timezone + "')";

    return datetime_representation;
}

/// class DateTime64Type

DateTime64Type::DateTime64Type(size_t precision, std::string timezone)
    : Type(DateTime64), details::TypeWithTimeZoneMixin(std::move(timezone)), precision_(precision) {

    if (precision_ > 18) {
        throw ValidationError("datetime64 precision is > 18");
    }
}

std::string DateTime64Type::GetName() const {
    std::string datetime64_representation;
    datetime64_representation.reserve(14);
    datetime64_representation += "datetime64(";
    datetime64_representation += std::to_string(precision_);

    const auto & timezone = Timezone();
    if (!timezone.empty()) {
        datetime64_representation += ", '" + timezone + "'";
    }

    datetime64_representation += ")";
    return datetime64_representation;
}

/// class FixedStringType

FixedStringType::FixedStringType(size_t n) : Type(FixedString), size_(n) {
}

/// class NullableType

NullableType::NullableType(TypeRef nested_type) : Type(Nullable), nested_type_(nested_type) {
}

/// class TupleType

TupleType::TupleType(const std::vector<TypeRef>& item_types) : Type(Tuple), item_types_(item_types) {
}

/// class LowCardinalityType
LowCardinalityType::LowCardinalityType(TypeRef nested_type) : Type(LowCardinality), nested_type_(nested_type) {
}

LowCardinalityType::~LowCardinalityType() {
}

std::string TupleType::GetName() const {
    std::string result("tuple(");

    if (!item_types_.empty()) {
        result += item_types_[0]->GetName();
    }

    for (size_t i = 1; i < item_types_.size(); ++i) {
        result += ", " + item_types_[i]->GetName();
    }

    result += ")";

    return result;
}

/// class MapType
MapType::MapType(TypeRef key_type, TypeRef value_type)
    : Type(Map)
    , key_type_(key_type)
    , value_type_(value_type) {
}

std::string MapType::GetName() const {
    return std::string("map(") + key_type_->GetName() + ", " +value_type_->GetName() + ")";
}

}  // namespace clickhouse
