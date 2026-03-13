#pragma once

#include "column.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace timeplus {

class ColumnDynamic : public Column {
public:
    static constexpr uint8_t NULL_DISCRIMINATOR = 0xFF;
    static constexpr uint64_t STRUCTURE_VERSION_V1 = 1;
    static constexpr uint64_t STRUCTURE_VERSION_V2 = 2;
    static constexpr uint64_t DISCRIMINATOR_MODE_BASIC = 0;
    static constexpr uint64_t DISCRIMINATOR_MODE_COMPACT = 1;

    explicit ColumnDynamic(size_t max_dynamic_types = Type::DEFAULT_DYNAMIC_MAX_TYPES);

    void Append(ColumnRef column) override;
    void Reserve(size_t new_cap) override;

    bool LoadPrefix(InputStream* input, size_t rows) override;
    bool LoadBody(InputStream* input, size_t rows) override;

    void SavePrefix(OutputStream* output) override;
    void SaveBody(OutputStream* output) override;

    void Clear() override;
    size_t Size() const override;

    ColumnRef Slice(size_t begin, size_t len) const override;
    ColumnRef CloneEmpty() const override;
    void Swap(Column& other) override;

    uint8_t GetDiscriminator(size_t row) const;
    size_t GetVariantOffset(size_t row) const;
    bool IsNull(size_t row) const;
    bool IsSharedVariant(size_t row) const;
    size_t GetSharedVariantDiscriminator() const;

    const std::vector<std::string>& GetVariantTypeNames() const;
    ColumnRef GetVariantColumn(size_t discriminator) const;

private:
    static constexpr const char* SHARED_VARIANT_TYPE_NAME = "shared_variant";

    bool IsCompatibleLayout(const ColumnDynamic& other) const;
    std::shared_ptr<ColumnDynamic> CloneStructureEmpty() const;

    bool ReadDiscriminators(InputStream* input, size_t rows, std::vector<uint8_t>* discriminators) const;
    static bool ReadDiscriminatorsBasic(InputStream* input, size_t rows, std::vector<uint8_t>* discriminators);
    static bool ReadDiscriminatorsCompact(InputStream* input, size_t rows, std::vector<uint8_t>* discriminators);

    void WriteDiscriminators(OutputStream* output) const;
    void WriteDiscriminatorsBasic(OutputStream* output) const;
    void WriteDiscriminatorsCompact(OutputStream* output) const;

    void ValidateSerializedState() const;

    size_t max_dynamic_types_;
    uint64_t structure_version_;
    uint64_t discriminator_mode_;
    size_t shared_variant_discriminator_;

    std::vector<std::string> variant_type_names_;
    std::vector<ColumnRef> variant_columns_;

    std::vector<uint8_t> discriminators_;
    std::vector<size_t> offsets_;
};

}  // namespace timeplus
