#include "decimal.h"

namespace
{
using namespace timeplus;

template <typename T>
inline bool getSignBit(const T & v)
{
    return v < static_cast<T>(0);
}

inline bool getSignBit(const Int256 & v)
{
   static constexpr Int256 zero {};
   return v < zero;

}

inline bool addOverflow(const Int256 & l, const Int256 & r, Int256 * result)
{
    // Based on code from:
    // https://wiki.sei.cmu.edu/confluence/display/c/INT32-C.+Ensure+that+operations+on+signed+integers+do+not+result+in+overflow#INT32C.Ensurethatoperationsonsignedintegersdonotresultinoverflow-CompliantSolution
    const auto r_positive = !getSignBit(r);

    if ((r_positive && (l > (std::numeric_limits<Int256>::max() - r))) ||
        (!r_positive && (l < (std::numeric_limits<Int256>::min() - r)))) {
        return true;
    }
    *result = l + r;

    return false;
}

template <typename T>
inline bool mulOverflow(const Int256 & l, const T & r, Int256 * result)
{
    // Based on code from:
    // https://wiki.sei.cmu.edu/confluence/display/c/INT32-C.+Ensure+that+operations+on+signed+integers+do+not+result+in+overflow#INT32C.Ensurethatoperationsonsignedintegersdonotresultinoverflow-CompliantSolution.3
    const auto l_positive = !getSignBit(l);
    const auto r_positive = !getSignBit(r);

    if (l_positive) {
        if (r_positive) {
            if (r != 0 && l > (std::numeric_limits<Int256>::max() / r)) {
                return true;
            }
        } else {
            if (l != 0 && r < (std::numeric_limits<Int256>::min() / l)) {
                return true;
            }
        }
    } else {
        if (r_positive) {
            if (r != 0 && l < (std::numeric_limits<Int256>::min() / r)) {
                return true;
            }
        } else {
            if (l != 0 && (r < (std::numeric_limits<Int256>::max() / l))) {
                return true;
            }
        }
    }

    *result = l * r;
    return false;
}

}

namespace timeplus {

ColumnDecimal::ColumnDecimal(size_t precision, size_t scale)
    : Column(Type::CreateDecimal(precision, scale))
{
    if (precision <= 9) {
        data_ = std::make_shared<ColumnInt32>();
    } else if (precision <= 18) {
        data_ = std::make_shared<ColumnInt64>();
    } else if (precision <= 38) {
        data_ = std::make_shared<ColumnInt128>();
    } else {
        data_ = std::make_shared<ColumnInt256>();
    }
}

ColumnDecimal::ColumnDecimal(TypeRef type, ColumnRef data)
    : Column(type),
      data_(data)
{
}

void ColumnDecimal::Append(const Int256& value) {
    if (data_->Type()->GetCode() == Type::Int32) {
        assert(value >= std::numeric_limits<ColumnInt32::DataType>::min() && value <= std::numeric_limits<ColumnInt32::DataType>::max());
        data_->As<ColumnInt32>()->Append(static_cast<ColumnInt32::DataType>(value));
    } else if (data_->Type()->GetCode() == Type::Int64) {
        assert(value >= std::numeric_limits<ColumnInt64::DataType>::min() && value <= std::numeric_limits<ColumnInt64::DataType>::max());
        data_->As<ColumnInt64>()->Append(static_cast<ColumnInt64::DataType>(value));
    } else if (data_->Type()->GetCode() == Type::Int128) {
        assert(value >= std::numeric_limits<ColumnInt128::DataType>::min() && value <= std::numeric_limits<ColumnInt128::DataType>::max());
        data_->As<ColumnInt128>()->Append(static_cast<ColumnInt128::DataType>(value));
    } else {
        assert(value >= std::numeric_limits<ColumnInt256::DataType>::min() && value <= std::numeric_limits<ColumnInt256::DataType>::max());
        data_->As<ColumnInt256>()->Append(static_cast<ColumnInt256::DataType>(value));
    }
}

void ColumnDecimal::Append(const std::string& value) {
    Int256 int_value = 0;
    auto c = value.begin();
    auto end = value.end();
    bool sign = true;
    bool has_dot = false;

    size_t int_part_length = 0;

    size_t zeros = 0;

    auto scale = type_->As<DecimalType>()->GetScale();
    auto precision = type_->As<DecimalType>()->GetPrecision();
    auto name = type_->As<DecimalType>()->GetName();

    while (c != end) {
        if (*c == '-') {
            sign = false;
            if (c != value.begin()) {
                break;
            }
        } else if (*c == '.' && !has_dot) {
            size_t distance = std::distance(c, end) - 1;

            if (distance <= scale) {
                zeros = scale - distance;
            } else {
                std::advance(end, scale - distance);
            }

            has_dot = true;
        } else if (*c >= '0' && *c <= '9') {
            if (int_part_length > precision - scale) {
                throw std::runtime_error("value is too big for " + std::string(name));
            }

            if (mulOverflow(int_value, 10, &int_value) ||
                addOverflow(int_value, *c - '0', &int_value)) {
                throw AssertionError("value is too big for " + std::string(name));
            }

            if (!has_dot) {
                int_part_length++;
            }
        } else {
            throw ValidationError(std::string("unexpected symbol '") + (*c) + "' in decimal value");
        }
        ++c;
    }

    if (c != end) {
        throw ValidationError("unexpected symbol '-' in decimal value");
    }

    while (zeros) {
        if (mulOverflow(int_value, 10, &int_value)) {
            throw AssertionError("value is too big for " + std::string(name));
        }
        --zeros;
    }

    Append(sign ? int_value : -int_value);
}

Int256 ColumnDecimal::At(size_t i) const {
    switch (data_->Type()->GetCode()) {
        case Type::Int32:
            return static_cast<Int256>(data_->As<ColumnInt32>()->At(i));
        case Type::Int64:
            return static_cast<Int256>(data_->As<ColumnInt64>()->At(i));
        case Type::Int128:
            return static_cast<Int256>(data_->As<ColumnInt128>()->At(i));
        case Type::Int256:
            return data_->As<ColumnInt256>()->At(i);
        default:
            throw ValidationError("Invalid data_ column type in ColumnDecimal");
    }
}

void ColumnDecimal::Reserve(size_t new_cap) {
    data_->Reserve(new_cap);
}

void ColumnDecimal::Append(ColumnRef column) {
    if (auto col = column->As<ColumnDecimal>()) {
        data_->Append(col->data_);
    }
}

bool ColumnDecimal::LoadBody(InputStream * input, size_t rows) {
    return data_->LoadBody(input, rows);
}

void ColumnDecimal::SaveBody(OutputStream* output) {
    data_->SaveBody(output);
}

void ColumnDecimal::Clear() {
    data_->Clear();
}

size_t ColumnDecimal::Size() const {
    return data_->Size();
}

ColumnRef ColumnDecimal::Slice(size_t begin, size_t len) const {
    // coundn't use std::make_shared since this c-tor is private
    return ColumnRef{new ColumnDecimal(type_, data_->Slice(begin, len))};
}

ColumnRef ColumnDecimal::CloneEmpty() const {
    // coundn't use std::make_shared since this c-tor is private
    return ColumnRef{new ColumnDecimal(type_, data_->CloneEmpty())};
}

void ColumnDecimal::Swap(Column& other) {
    auto & col = dynamic_cast<ColumnDecimal &>(other);
    data_.swap(col.data_);
}

ItemView ColumnDecimal::GetItem(size_t index) const {
    return ItemView{GetType().GetCode(), data_->GetItem(index)};
}

size_t ColumnDecimal::GetScale() const
{
    return type_->As<DecimalType>()->GetScale();
}

size_t ColumnDecimal::GetPrecision() const
{
    return type_->As<DecimalType>()->GetPrecision();
}

}
