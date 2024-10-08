#pragma once

#include "column.h"
#include "numeric.h"

namespace timeplus {

/**
 * Represents a column of decimal type.
 */
class ColumnDecimal : public Column {
public:
    using ValueType = Int256;

    ColumnDecimal(size_t precision, size_t scale);

    void Append(const Int256& value);
    void Append(const std::string& value);

    Int256 At(size_t i) const;
    inline auto operator[](size_t i) const { return At(i); }

public:
    /// Increase the capacity of the column for large block insertion.
    void Reserve(size_t new_cap) override;
    void Append(ColumnRef column) override;
    bool LoadBody(InputStream* input, size_t rows) override;
    void SaveBody(OutputStream* output) override;
    void Clear() override;
    size_t Size() const override;
    ColumnRef Slice(size_t begin, size_t len) const override;
    ColumnRef CloneEmpty() const override;
    void Swap(Column& other) override;
    ItemView GetItem(size_t index) const override;

    size_t GetScale() const;
    size_t GetPrecision() const;

private:
    /// Depending on a precision it can be one of:
    ///  - ColumnInt32
    ///  - ColumnInt64
    ///  - ColumnInt128
    ColumnRef data_;

    explicit ColumnDecimal(TypeRef type, ColumnRef data);
};

}
