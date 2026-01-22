#include <timeplus/base/wire_format.h>
#include <timeplus/base/output.h>
#include <timeplus/base/input.h>

#include <gtest/gtest.h>
#include <string>

using namespace timeplus;

namespace {
    std::string roundtripQuotedString(const std::string& value) {
        Buffer buf;
        {
            BufferOutput output(&buf);
            WireFormat::WriteQuotedString(output, value);
            output.Flush();
        }
        ArrayInput input(buf.data(), buf.size());
        std::string result;
        if (!WireFormat::ReadString(input, &result)) {
            return {};
        }
        return result;
    }
}

TEST(CodedStreamCase, Varint64) {
    Buffer buf;

    {
        BufferOutput output(&buf);
        WireFormat::WriteVarint64(output, 18446744071965638648ULL);
        output.Flush();
    }

    {
        ArrayInput input(buf.data(), buf.size());
        uint64_t value = 0;
        ASSERT_TRUE(WireFormat::ReadVarint64(input, &value));
        ASSERT_EQ(value, 18446744071965638648ULL);
    }
}

TEST(CodedStreamCase, QuotedStringPlain) {
    ASSERT_EQ(roundtripQuotedString("hello"), "'hello'");
}

TEST(CodedStreamCase, QuotedStringSingleQuote) {
    ASSERT_EQ(roundtripQuotedString("a'b"), "'a\\x27b'");
}

TEST(CodedStreamCase, QuotedStringNullByte) {
    const std::string value("a\0b", 3);
    ASSERT_EQ(roundtripQuotedString(value), "'a\\x00b'");
}
