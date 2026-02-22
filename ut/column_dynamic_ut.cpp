#include <timeplus/columns/dynamic.h>
#include <timeplus/columns/string.h>
#include <timeplus/base/input.h>
#include <timeplus/base/output.h>
#include <timeplus/base/wire_format.h>
#include <timeplus/exceptions.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <array>
#include <string>
#include <vector>

namespace {

using namespace timeplus;

std::string BuildDynamicSerializedData(
    uint64_t structure_version,
    const std::vector<std::string>& variant_type_names,
    uint64_t discriminator_mode,
    const std::vector<uint8_t>& discriminators,
    const std::vector<std::vector<std::string>>& variant_values) {
    std::array<char, 16 * 1024> buffer{};
    ArrayOutput output(buffer.data(), buffer.size());

    const auto num_dynamic_types = static_cast<uint64_t>(variant_type_names.size());

    WireFormat::WriteFixed(output, structure_version);
    if (structure_version == ColumnDynamic::STRUCTURE_VERSION_V1) {
        WireFormat::WriteUInt64(output, num_dynamic_types);
    }

    WireFormat::WriteUInt64(output, num_dynamic_types);
    for (const auto& variant_type_name : variant_type_names) {
        WireFormat::WriteString(output, variant_type_name);
    }

    WireFormat::WriteFixed(output, discriminator_mode);

    std::vector<std::string> source_variant_names = variant_type_names;
    source_variant_names.push_back("shared_variant");
    EXPECT_EQ(source_variant_names.size(), variant_values.size());

    std::vector<size_t> sorted_indices(source_variant_names.size());
    for (size_t i = 0; i < sorted_indices.size(); ++i) {
        sorted_indices[i] = i;
    }

    std::sort(sorted_indices.begin(), sorted_indices.end(), [&](size_t lhs, size_t rhs) {
        return source_variant_names[lhs] < source_variant_names[rhs];
    });

    std::vector<uint8_t> source_to_serialized_discriminator(source_variant_names.size(), 0);
    for (size_t serialized_discriminator = 0; serialized_discriminator < sorted_indices.size(); ++serialized_discriminator) {
        source_to_serialized_discriminator[sorted_indices[serialized_discriminator]] =
            static_cast<uint8_t>(serialized_discriminator);
    }

    auto map_discriminator = [&](uint8_t discriminator) {
        if (discriminator == ColumnDynamic::NULL_DISCRIMINATOR) {
            return discriminator;
        }

        if (discriminator >= source_to_serialized_discriminator.size()) {
            return discriminator;
        }

        return source_to_serialized_discriminator[discriminator];
    };

    if (discriminator_mode == ColumnDynamic::DISCRIMINATOR_MODE_BASIC) {
        for (const auto discriminator : discriminators) {
            WireFormat::WriteFixed(output, map_discriminator(discriminator));
        }
    } else {
        WireFormat::WriteUInt64(output, discriminators.size());
        WireFormat::WriteFixed(output, static_cast<uint8_t>(0));
        for (const auto discriminator : discriminators) {
            WireFormat::WriteFixed(output, map_discriminator(discriminator));
        }
    }

    for (const auto variant_index : sorted_indices) {
        for (const auto& value : variant_values[variant_index]) {
            WireFormat::WriteString(output, value);
        }
    }

    return std::string(buffer.data(), output.Size());
}

std::shared_ptr<ColumnDynamic> LoadDynamicColumn(const std::string& serialized_data, size_t rows, size_t max_types = 1) {
    auto col = std::make_shared<ColumnDynamic>(max_types);
    ArrayInput input(serialized_data.data(), serialized_data.size());
    EXPECT_TRUE(col->Load(&input, rows));
    return col;
}

}  // namespace

TEST(ColumnDynamic, LoadV1Basic) {
    const std::string shared_0 = std::string("\x2b\x03", 2);
    const std::string shared_1 = std::string("\x2b\x04\xff", 3);

    const auto serialized = BuildDynamicSerializedData(
        ColumnDynamic::STRUCTURE_VERSION_V1,
        {"string"},
        ColumnDynamic::DISCRIMINATOR_MODE_BASIC,
        {0, ColumnDynamic::NULL_DISCRIMINATOR, 1, 1},
        {
            {"alpha"},
            {shared_0, shared_1},
        });

    auto col = LoadDynamicColumn(serialized, 4);

    ASSERT_EQ(4u, col->Size());
    ASSERT_EQ(2u, col->GetVariantTypeNames().size());
    EXPECT_EQ("shared_variant", col->GetVariantTypeNames()[0]);
    EXPECT_EQ("string", col->GetVariantTypeNames()[1]);

    const auto shared_discriminator = static_cast<uint8_t>(col->GetSharedVariantDiscriminator());
    const auto string_discriminator = static_cast<uint8_t>(shared_discriminator == 0 ? 1 : 0);

    EXPECT_EQ(string_discriminator, col->GetDiscriminator(0));
    EXPECT_EQ(ColumnDynamic::NULL_DISCRIMINATOR, col->GetDiscriminator(1));
    EXPECT_EQ(shared_discriminator, col->GetDiscriminator(2));
    EXPECT_EQ(shared_discriminator, col->GetDiscriminator(3));

    EXPECT_FALSE(col->IsNull(0));
    EXPECT_TRUE(col->IsNull(1));
    EXPECT_TRUE(col->IsSharedVariant(2));
    EXPECT_TRUE(col->IsSharedVariant(3));

    auto string_variant = col->GetVariantColumn(string_discriminator)->As<ColumnString>();
    auto shared_variant = col->GetVariantColumn(shared_discriminator)->As<ColumnString>();

    ASSERT_NE(nullptr, string_variant);
    ASSERT_NE(nullptr, shared_variant);
    ASSERT_EQ(1u, string_variant->Size());
    ASSERT_EQ(2u, shared_variant->Size());
    EXPECT_EQ("alpha", string_variant->At(0));
    EXPECT_EQ(shared_0, shared_variant->At(0));
    EXPECT_EQ(shared_1, shared_variant->At(1));
}

TEST(ColumnDynamic, LoadV2Basic) {
    const auto serialized = BuildDynamicSerializedData(
        ColumnDynamic::STRUCTURE_VERSION_V2,
        {"string"},
        ColumnDynamic::DISCRIMINATOR_MODE_BASIC,
        {1, 0, ColumnDynamic::NULL_DISCRIMINATOR},
        {
            {"from_variant"},
            {std::string("\x2b\x07\x01", 3)},
        });

    auto col = LoadDynamicColumn(serialized, 3);

    ASSERT_EQ(3u, col->Size());
    const auto shared_discriminator = static_cast<uint8_t>(col->GetSharedVariantDiscriminator());
    const auto string_discriminator = static_cast<uint8_t>(shared_discriminator == 0 ? 1 : 0);

    EXPECT_EQ(shared_discriminator, col->GetDiscriminator(0));
    EXPECT_EQ(string_discriminator, col->GetDiscriminator(1));
    EXPECT_TRUE(col->IsSharedVariant(0));
    EXPECT_FALSE(col->IsSharedVariant(1));
    EXPECT_TRUE(col->IsNull(2));
}

TEST(ColumnDynamic, SaveAfterLoadPreservesBytes) {
    const auto serialized = BuildDynamicSerializedData(
        ColumnDynamic::STRUCTURE_VERSION_V1,
        {"string"},
        ColumnDynamic::DISCRIMINATOR_MODE_BASIC,
        {0, ColumnDynamic::NULL_DISCRIMINATOR, 1, 1},
        {
            {"alpha"},
            {
                std::string("\x2b\x03", 2),
                std::string("\x2b\x04\xff", 3),
            },
        });

    auto col = LoadDynamicColumn(serialized, 4);

    std::array<char, 16 * 1024> buffer{};
    ArrayOutput output(buffer.data(), buffer.size());
    col->Save(&output);

    const std::string roundtripped(buffer.data(), output.Size());
    EXPECT_EQ(serialized, roundtripped);
}

TEST(ColumnDynamic, SliceAndAppendPreserveLayout) {
    const auto serialized = BuildDynamicSerializedData(
        ColumnDynamic::STRUCTURE_VERSION_V1,
        {"string"},
        ColumnDynamic::DISCRIMINATOR_MODE_BASIC,
        {0, ColumnDynamic::NULL_DISCRIMINATOR, 1, 1},
        {
            {"alpha"},
            {
                std::string("\x2b\x03", 2),
                std::string("\x2b\x04\xff", 3),
            },
        });

    auto col = LoadDynamicColumn(serialized, 4);

    auto first = col->Slice(0, 2)->As<ColumnDynamic>();
    auto second = col->Slice(2, 2)->As<ColumnDynamic>();

    ASSERT_NE(nullptr, first);
    ASSERT_NE(nullptr, second);

    first->Append(second);

    ASSERT_EQ(4u, first->Size());
    const auto shared_discriminator = static_cast<uint8_t>(first->GetSharedVariantDiscriminator());
    const auto string_discriminator = static_cast<uint8_t>(shared_discriminator == 0 ? 1 : 0);

    EXPECT_EQ(string_discriminator, first->GetDiscriminator(0));
    EXPECT_EQ(ColumnDynamic::NULL_DISCRIMINATOR, first->GetDiscriminator(1));
    EXPECT_EQ(shared_discriminator, first->GetDiscriminator(2));
    EXPECT_EQ(shared_discriminator, first->GetDiscriminator(3));

    auto string_variant = first->GetVariantColumn(string_discriminator)->As<ColumnString>();
    auto shared_variant = first->GetVariantColumn(shared_discriminator)->As<ColumnString>();
    ASSERT_NE(nullptr, string_variant);
    ASSERT_NE(nullptr, shared_variant);

    EXPECT_EQ(1u, string_variant->Size());
    EXPECT_EQ(2u, shared_variant->Size());
    EXPECT_EQ(std::string("\x2b\x03", 2), shared_variant->At(0));
    EXPECT_EQ(std::string("\x2b\x04\xff", 3), shared_variant->At(1));
}

TEST(ColumnDynamic, RejectInvalidDiscriminator) {
    const auto serialized = BuildDynamicSerializedData(
        ColumnDynamic::STRUCTURE_VERSION_V1,
        {"string"},
        ColumnDynamic::DISCRIMINATOR_MODE_BASIC,
        {5},
        {
            {},
            {},
        });

    auto col = std::make_shared<ColumnDynamic>(1);
    ArrayInput input(serialized.data(), serialized.size());
    EXPECT_THROW(col->Load(&input, 1), ProtocolError);
}

TEST(ColumnDynamic, RejectUnsupportedVariantType) {
    const auto serialized = BuildDynamicSerializedData(
        ColumnDynamic::STRUCTURE_VERSION_V1,
        {"variant(uint8, string)"},
        ColumnDynamic::DISCRIMINATOR_MODE_BASIC,
        {},
        {
            {},
            {},
        });

    auto col = std::make_shared<ColumnDynamic>(1);
    ArrayInput input(serialized.data(), serialized.size());
    EXPECT_THROW(col->Load(&input, 0), UnimplementedError);
}
