#pragma once

#include "columns/column.h"

namespace timeplus {

struct BlockInfo {
    uint8_t is_overflows = 0;
    int32_t bucket_num = -1;
};

class Block {
public:
    /// Allow to iterate over block's columns.
    class Iterator {
    public:
        Iterator(const Block& block);

        /// Name of column.
        const std::string& Name() const;

        /// Type of column.
        TypeRef Type() const;

        /// Reference to column object.
        ColumnRef Column() const;

        /// Move to next column, returns false if next call to IsValid() would return false;
        bool Next();

        /// Is the iterator still valid.
        bool IsValid() const;

        size_t ColumnIndex() const {
            return idx_;
        }

        Iterator& operator*() { return *this; }
        const Iterator& operator*() const { return *this; }

        bool operator==(const Iterator & other) const {
            return &block_ == &other.block_ && idx_ == other.idx_;
        }
        bool operator!=(const Iterator & other) const {
            return !(*this == other);
        }

        Iterator& operator++() {
            this->Next();
            return *this;
        }

    private:
        friend class Block;
        struct ConstructAtEndTag {};
        Iterator(const Block& block, ConstructAtEndTag at_end);
        Iterator() = delete;

        const Block& block_;
        size_t idx_;
    };

public:
     Block();
     Block(size_t cols, size_t rows);
    ~Block();

    /// Append named column to the block.
    void AppendColumn(const std::string& name, const ColumnRef& col);

    /// Count of columns in the block.
    size_t GetColumnCount() const;

    const BlockInfo& Info() const;

    /// Set block info
    void SetInfo(BlockInfo info);

    /// Count of rows in the block.
    size_t GetRowCount() const;

    size_t RefreshRowCount();

    const std::string& GetColumnName(size_t idx) const {
        return columns_.at(idx).name;
    }

    /// Reference to column by index in the block.
    ColumnRef operator [] (size_t idx) const;

    Iterator begin() const;
    Iterator end() const;
    Iterator cbegin() const { return begin(); }
    Iterator cend() const { return end(); }

private:
    struct ColumnItem {
        std::string name;
        ColumnRef   column;
    };

    BlockInfo info_;
    std::vector<ColumnItem> columns_;
    /// Count of rows in the block.
    size_t rows_;
};

using BlockPtr = std::shared_ptr<Block>;

}
