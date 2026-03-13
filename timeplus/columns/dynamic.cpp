#include "dynamic.h"

#include "factory.h"
#include "string.h"

#include "../base/wire_format.h"
#include "../exceptions.h"

#include <algorithm>
#include <limits>
#include <vector>

namespace timeplus {
namespace {

constexpr uint8_t COMPACT_GRANULE_PLAIN = 0;
constexpr uint8_t COMPACT_GRANULE_SINGLE_DISCRIMINATOR = 1;

}  // namespace

ColumnDynamic::ColumnDynamic(size_t max_dynamic_types)
    : Column(Type::CreateDynamic(max_dynamic_types))
    , max_dynamic_types_(max_dynamic_types)
    , structure_version_(STRUCTURE_VERSION_V1)
    , discriminator_mode_(DISCRIMINATOR_MODE_BASIC)
    , shared_variant_discriminator_(0)
{
    variant_type_names_.push_back(SHARED_VARIANT_TYPE_NAME);
    variant_columns_.push_back(std::make_shared<ColumnString>());
}

void ColumnDynamic::Append(ColumnRef column) {
    auto other = column->As<ColumnDynamic>();
    if (!other) {
        return;
    }

    if (!IsCompatibleLayout(*other)) {
        throw ValidationError("can't append dynamic columns with different layout");
    }

    if (other->Size() == 0) {
        return;
    }

    std::vector<size_t> base_offsets(variant_columns_.size(), 0);
    for (size_t i = 0; i < variant_columns_.size(); ++i) {
        base_offsets[i] = variant_columns_[i]->Size();
        variant_columns_[i]->Append(other->variant_columns_[i]);
    }

    discriminators_.reserve(discriminators_.size() + other->discriminators_.size());
    offsets_.reserve(offsets_.size() + other->offsets_.size());
    for (size_t i = 0; i < other->Size(); ++i) {
        const auto discriminator = other->discriminators_[i];
        discriminators_.push_back(discriminator);

        if (discriminator == NULL_DISCRIMINATOR) {
            offsets_.push_back(0);
        } else {
            offsets_.push_back(base_offsets[discriminator] + other->offsets_[i]);
        }
    }
}

void ColumnDynamic::Reserve(size_t new_cap) {
    discriminators_.reserve(new_cap);
    offsets_.reserve(new_cap);
    for (auto& variant_column : variant_columns_) {
        variant_column->Reserve(new_cap);
    }
}

bool ColumnDynamic::LoadPrefix(InputStream* input, size_t rows) {
    uint64_t structure_version = 0;
    if (!WireFormat::ReadFixed(*input, &structure_version)) {
        return false;
    }

    if (structure_version != STRUCTURE_VERSION_V1 && structure_version != STRUCTURE_VERSION_V2) {
        throw ProtocolError("Invalid dynamic structure serialization version: " + std::to_string(structure_version));
    }

    if (structure_version == STRUCTURE_VERSION_V1) {
        uint64_t ignored = 0;
        if (!WireFormat::ReadUInt64(*input, &ignored)) {
            return false;
        }
    }

    uint64_t num_dynamic_types = 0;
    if (!WireFormat::ReadUInt64(*input, &num_dynamic_types)) {
        return false;
    }

    if (num_dynamic_types > Type::MAX_DYNAMIC_TYPES_LIMIT) {
        throw ProtocolError("Dynamic type count is out of range: " + std::to_string(num_dynamic_types));
    }

    struct VariantEntry {
        std::string type_name;
        ColumnRef column;
    };

    std::vector<VariantEntry> variants;
    variants.reserve(static_cast<size_t>(num_dynamic_types) + 1);

    for (size_t i = 0; i < static_cast<size_t>(num_dynamic_types); ++i) {
        std::string type_name;
        if (!WireFormat::ReadString(*input, &type_name)) {
            return false;
        }

        if (type_name == SHARED_VARIANT_TYPE_NAME) {
            throw ProtocolError("Unexpected dynamic variant type name: " + type_name);
        }

        auto variant_column = CreateColumnByType(type_name);
        if (!variant_column) {
            throw UnimplementedError("unsupported dynamic variant type: " + type_name);
        }

        variants.push_back({type_name, variant_column});
    }

    variants.push_back({SHARED_VARIANT_TYPE_NAME, std::make_shared<ColumnString>()});

    // Proton serializes Dynamic discriminators in Variant global order, where
    // variants are sorted lexicographically by type name.
    std::sort(variants.begin(), variants.end(), [](const VariantEntry& lhs, const VariantEntry& rhs) {
        return lhs.type_name < rhs.type_name;
    });

    std::vector<std::string> variant_type_names;
    std::vector<ColumnRef> variant_columns;
    variant_type_names.reserve(variants.size());
    variant_columns.reserve(variants.size());

    size_t shared_variant_discriminator = std::numeric_limits<size_t>::max();
    for (size_t i = 0; i < variants.size(); ++i) {
        variant_type_names.push_back(std::move(variants[i].type_name));
        variant_columns.push_back(std::move(variants[i].column));
        if (variant_type_names.back() == SHARED_VARIANT_TYPE_NAME) {
            shared_variant_discriminator = i;
        }
    }

    uint64_t discriminator_mode = 0;
    if (!WireFormat::ReadFixed(*input, &discriminator_mode)) {
        return false;
    }

    if (discriminator_mode != DISCRIMINATOR_MODE_BASIC
            && discriminator_mode != DISCRIMINATOR_MODE_COMPACT) {
        throw ProtocolError("Invalid dynamic discriminator serialization mode: " + std::to_string(discriminator_mode));
    }

    if (shared_variant_discriminator == std::numeric_limits<size_t>::max()) {
        throw ProtocolError("Dynamic shared variant is missing in serialization");
    }

    if (!variant_columns[shared_variant_discriminator]->As<ColumnString>()) {
        throw ProtocolError("Dynamic shared variant must use string serialization");
    }

    for (auto& variant_column : variant_columns) {
        if (!variant_column->LoadPrefix(input, rows)) {
            return false;
        }
    }

    structure_version_ = structure_version;
    discriminator_mode_ = discriminator_mode;
    shared_variant_discriminator_ = shared_variant_discriminator;
    variant_type_names_ = std::move(variant_type_names);
    variant_columns_ = std::move(variant_columns);

    discriminators_.clear();
    offsets_.clear();
    return true;
}

bool ColumnDynamic::LoadBody(InputStream* input, size_t rows) {
    ValidateSerializedState();

    std::vector<uint8_t> new_discriminators;
    if (!ReadDiscriminators(input, rows, &new_discriminators)) {
        return false;
    }

    std::vector<size_t> variant_sizes(variant_columns_.size(), 0);
    std::vector<size_t> new_offsets(rows, 0);

    for (size_t row = 0; row < rows; ++row) {
        const auto discriminator = new_discriminators[row];
        if (discriminator == NULL_DISCRIMINATOR) {
            continue;
        }

        if (discriminator >= variant_columns_.size()) {
            throw ProtocolError("Invalid dynamic discriminator: " + std::to_string(discriminator));
        }

        new_offsets[row] = variant_sizes[discriminator];
        ++variant_sizes[discriminator];
    }

    for (auto& variant_column : variant_columns_) {
        variant_column->Clear();
    }

    for (size_t i = 0; i < variant_columns_.size(); ++i) {
        if (variant_sizes[i] == 0) {
            continue;
        }

        if (!variant_columns_[i]->LoadBody(input, variant_sizes[i])) {
            return false;
        }
    }

    discriminators_ = std::move(new_discriminators);
    offsets_ = std::move(new_offsets);
    return true;
}

void ColumnDynamic::SavePrefix(OutputStream* output) {
    ValidateSerializedState();

    const auto num_dynamic_types = static_cast<uint64_t>(variant_columns_.size() - 1);

    // Use V1 for better compatibility with old server/client revisions.
    WireFormat::WriteFixed(*output, static_cast<uint64_t>(STRUCTURE_VERSION_V1));
    WireFormat::WriteUInt64(*output, num_dynamic_types);
    WireFormat::WriteUInt64(*output, num_dynamic_types);

    for (size_t i = 0; i < variant_type_names_.size(); ++i) {
        if (i == shared_variant_discriminator_) {
            continue;
        }

        WireFormat::WriteString(*output, variant_type_names_[i]);
    }

    WireFormat::WriteFixed(*output, discriminator_mode_);

    for (auto& variant_column : variant_columns_) {
        variant_column->SavePrefix(output);
    }
}

void ColumnDynamic::SaveBody(OutputStream* output) {
    ValidateSerializedState();

    std::vector<size_t> variant_sizes(variant_columns_.size(), 0);
    for (size_t row = 0; row < Size(); ++row) {
        const auto discriminator = discriminators_[row];
        if (discriminator == NULL_DISCRIMINATOR) {
            continue;
        }

        if (discriminator >= variant_columns_.size()) {
            throw ValidationError("Invalid dynamic discriminator value: " + std::to_string(discriminator));
        }

        if (offsets_[row] != variant_sizes[discriminator]) {
            throw ValidationError("Invalid dynamic row offset for discriminator: " + std::to_string(discriminator));
        }

        ++variant_sizes[discriminator];
    }

    for (size_t i = 0; i < variant_columns_.size(); ++i) {
        if (variant_columns_[i]->Size() != variant_sizes[i]) {
            throw ValidationError("Invalid dynamic variant size for type: " + variant_type_names_[i]);
        }
    }

    WriteDiscriminators(output);

    for (size_t i = 0; i < variant_columns_.size(); ++i) {
        if (variant_sizes[i] == 0) {
            continue;
        }

        variant_columns_[i]->SaveBody(output);
    }
}

void ColumnDynamic::Clear() {
    discriminators_.clear();
    offsets_.clear();
    for (auto& variant_column : variant_columns_) {
        variant_column->Clear();
    }
}

size_t ColumnDynamic::Size() const {
    return discriminators_.size();
}

ColumnRef ColumnDynamic::Slice(size_t begin, size_t len) const {
    if (len && begin + len > Size()) {
        throw ValidationError("Slice indexes are out of bounds");
    }

    auto result = CloneStructureEmpty();
    if (len == 0) {
        return result;
    }

    result->discriminators_.reserve(len);
    result->offsets_.reserve(len);

    std::vector<size_t> variant_sizes(variant_columns_.size(), 0);
    std::vector<size_t> first_offsets(variant_columns_.size(), std::numeric_limits<size_t>::max());

    for (size_t row = begin; row < begin + len; ++row) {
        const auto discriminator = discriminators_[row];

        result->discriminators_.push_back(discriminator);

        if (discriminator == NULL_DISCRIMINATOR) {
            result->offsets_.push_back(0);
            continue;
        }

        if (discriminator >= variant_columns_.size()) {
            throw ValidationError("Invalid dynamic discriminator value: " + std::to_string(discriminator));
        }

        if (first_offsets[discriminator] == std::numeric_limits<size_t>::max()) {
            first_offsets[discriminator] = offsets_[row];
        }

        result->offsets_.push_back(variant_sizes[discriminator]);
        ++variant_sizes[discriminator];
    }

    for (size_t i = 0; i < variant_columns_.size(); ++i) {
        if (variant_sizes[i] == 0) {
            continue;
        }

        result->variant_columns_[i]->Append(variant_columns_[i]->Slice(first_offsets[i], variant_sizes[i]));
    }

    return result;
}

ColumnRef ColumnDynamic::CloneEmpty() const {
    return CloneStructureEmpty();
}

void ColumnDynamic::Swap(Column& other) {
    auto& col = dynamic_cast<ColumnDynamic&>(other);
    if (!IsCompatibleLayout(col)) {
        throw ValidationError("can't swap dynamic columns with different layout");
    }

    discriminators_.swap(col.discriminators_);
    offsets_.swap(col.offsets_);
    variant_columns_.swap(col.variant_columns_);
    std::swap(shared_variant_discriminator_, col.shared_variant_discriminator_);
}

uint8_t ColumnDynamic::GetDiscriminator(size_t row) const {
    if (row >= Size()) {
        throw ValidationError("Index is out of bounds: " + std::to_string(row));
    }

    return discriminators_[row];
}

size_t ColumnDynamic::GetVariantOffset(size_t row) const {
    if (row >= Size()) {
        throw ValidationError("Index is out of bounds: " + std::to_string(row));
    }

    return offsets_[row];
}

bool ColumnDynamic::IsNull(size_t row) const {
    return GetDiscriminator(row) == NULL_DISCRIMINATOR;
}

bool ColumnDynamic::IsSharedVariant(size_t row) const {
    return !IsNull(row) && GetDiscriminator(row) == GetSharedVariantDiscriminator();
}

size_t ColumnDynamic::GetSharedVariantDiscriminator() const {
    if (variant_columns_.empty() || shared_variant_discriminator_ >= variant_columns_.size()) {
        throw ValidationError("Dynamic column has no variant columns");
    }

    return shared_variant_discriminator_;
}

const std::vector<std::string>& ColumnDynamic::GetVariantTypeNames() const {
    return variant_type_names_;
}

ColumnRef ColumnDynamic::GetVariantColumn(size_t discriminator) const {
    if (discriminator >= variant_columns_.size()) {
        throw ValidationError("Dynamic variant index is out of bounds: " + std::to_string(discriminator));
    }

    return variant_columns_[discriminator];
}

bool ColumnDynamic::IsCompatibleLayout(const ColumnDynamic& other) const {
    if (!Type()->IsEqual(other.Type())) {
        return false;
    }

    if (structure_version_ != other.structure_version_ || discriminator_mode_ != other.discriminator_mode_) {
        return false;
    }

    if (variant_type_names_ != other.variant_type_names_) {
        return false;
    }

    if (shared_variant_discriminator_ != other.shared_variant_discriminator_) {
        return false;
    }

    if (variant_columns_.size() != other.variant_columns_.size()) {
        return false;
    }

    for (size_t i = 0; i < variant_columns_.size(); ++i) {
        if (!variant_columns_[i]->Type()->IsEqual(other.variant_columns_[i]->Type())) {
            return false;
        }
    }

    return true;
}

std::shared_ptr<ColumnDynamic> ColumnDynamic::CloneStructureEmpty() const {
    auto result = std::make_shared<ColumnDynamic>(max_dynamic_types_);

    result->structure_version_ = structure_version_;
    result->discriminator_mode_ = discriminator_mode_;
    result->shared_variant_discriminator_ = shared_variant_discriminator_;
    result->variant_type_names_ = variant_type_names_;

    result->variant_columns_.clear();
    result->variant_columns_.reserve(variant_columns_.size());
    for (const auto& variant_column : variant_columns_) {
        result->variant_columns_.push_back(variant_column->CloneEmpty());
    }

    return result;
}

bool ColumnDynamic::ReadDiscriminators(InputStream* input, size_t rows, std::vector<uint8_t>* discriminators) const {
    switch (discriminator_mode_) {
        case DISCRIMINATOR_MODE_BASIC:
            return ReadDiscriminatorsBasic(input, rows, discriminators);
        case DISCRIMINATOR_MODE_COMPACT:
            return ReadDiscriminatorsCompact(input, rows, discriminators);
        default:
            throw ProtocolError("Invalid dynamic discriminator serialization mode: " + std::to_string(discriminator_mode_));
    }
}

bool ColumnDynamic::ReadDiscriminatorsBasic(InputStream* input, size_t rows, std::vector<uint8_t>* discriminators) {
    discriminators->assign(rows, 0);

    for (size_t i = 0; i < rows; ++i) {
        if (!WireFormat::ReadFixed(*input, &(*discriminators)[i])) {
            return false;
        }
    }

    return true;
}

bool ColumnDynamic::ReadDiscriminatorsCompact(InputStream* input, size_t rows, std::vector<uint8_t>* discriminators) {
    discriminators->clear();
    discriminators->reserve(rows);

    size_t read_rows = 0;
    while (read_rows < rows) {
        uint64_t granule_rows = 0;
        if (!WireFormat::ReadUInt64(*input, &granule_rows)) {
            return false;
        }

        if (granule_rows == 0) {
            throw ProtocolError("Invalid compact discriminator granule size: 0");
        }

        if (read_rows + granule_rows > rows) {
            throw ProtocolError("Invalid compact discriminator granule size: " + std::to_string(granule_rows));
        }

        uint8_t granule_format = 0;
        if (!WireFormat::ReadFixed(*input, &granule_format)) {
            return false;
        }

        if (granule_format == COMPACT_GRANULE_PLAIN) {
            for (size_t i = 0; i < static_cast<size_t>(granule_rows); ++i) {
                uint8_t discriminator = 0;
                if (!WireFormat::ReadFixed(*input, &discriminator)) {
                    return false;
                }
                discriminators->push_back(discriminator);
            }
        } else if (granule_format == COMPACT_GRANULE_SINGLE_DISCRIMINATOR) {
            uint8_t discriminator = 0;
            if (!WireFormat::ReadFixed(*input, &discriminator)) {
                return false;
            }

            discriminators->insert(
                discriminators->end(),
                static_cast<size_t>(granule_rows),
                discriminator);
        } else {
            throw ProtocolError("Invalid compact discriminator granule format: " + std::to_string(granule_format));
        }

        read_rows += static_cast<size_t>(granule_rows);
    }

    return true;
}

void ColumnDynamic::WriteDiscriminators(OutputStream* output) const {
    switch (discriminator_mode_) {
        case DISCRIMINATOR_MODE_BASIC:
            WriteDiscriminatorsBasic(output);
            return;
        case DISCRIMINATOR_MODE_COMPACT:
            WriteDiscriminatorsCompact(output);
            return;
        default:
            throw ValidationError("Invalid dynamic discriminator serialization mode: " + std::to_string(discriminator_mode_));
    }
}

void ColumnDynamic::WriteDiscriminatorsBasic(OutputStream* output) const {
    for (const auto discriminator : discriminators_) {
        WireFormat::WriteFixed(*output, discriminator);
    }
}

void ColumnDynamic::WriteDiscriminatorsCompact(OutputStream* output) const {
    if (discriminators_.empty()) {
        return;
    }

    WireFormat::WriteUInt64(*output, discriminators_.size());

    const auto first_discriminator = discriminators_[0];
    bool same_discriminator = true;
    for (const auto discriminator : discriminators_) {
        if (discriminator != first_discriminator) {
            same_discriminator = false;
            break;
        }
    }

    if (same_discriminator) {
        WireFormat::WriteFixed(*output, static_cast<uint8_t>(COMPACT_GRANULE_SINGLE_DISCRIMINATOR));
        WireFormat::WriteFixed(*output, first_discriminator);
        return;
    }

    WireFormat::WriteFixed(*output, static_cast<uint8_t>(COMPACT_GRANULE_PLAIN));
    for (const auto discriminator : discriminators_) {
        WireFormat::WriteFixed(*output, discriminator);
    }
}

void ColumnDynamic::ValidateSerializedState() const {
    if (max_dynamic_types_ > Type::MAX_DYNAMIC_TYPES_LIMIT) {
        throw ValidationError("dynamic max_types is out of range");
    }

    if (variant_type_names_.size() != variant_columns_.size()) {
        throw ValidationError("dynamic variant names and columns count mismatch");
    }

    if (variant_columns_.empty()) {
        throw ValidationError("dynamic column has no variants");
    }

    if (shared_variant_discriminator_ >= variant_type_names_.size()) {
        throw ValidationError("dynamic shared variant index is out of bounds");
    }

    if (variant_type_names_[shared_variant_discriminator_] != SHARED_VARIANT_TYPE_NAME) {
        throw ValidationError("dynamic shared variant is missing");
    }

    if (!variant_columns_[shared_variant_discriminator_]->As<ColumnString>()) {
        throw ValidationError("dynamic shared variant must be string column");
    }

    if (discriminators_.size() != offsets_.size()) {
        throw ValidationError("dynamic discriminators and offsets size mismatch");
    }
}

}  // namespace timeplus
