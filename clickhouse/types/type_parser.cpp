#include "type_parser.h"

#include "clickhouse/exceptions.h"
#include "clickhouse/base/platform.h" // for _win_

#include <algorithm>
#include <cmath>
#include <map>
#include <mutex>
#include <unordered_map>

#if defined _win_
#include <string.h>
#else
#include <strings.h>
#endif


namespace clickhouse {

bool TypeAst::operator==(const TypeAst & other) const {
    return meta == other.meta
        && code == other.code
        && name == other.name
        && value == other.value
        && std::equal(elements.begin(), elements.end(), other.elements.begin(), other.elements.end());
}

static const std::unordered_map<std::string, Type::Code> kTypeCode = {
    { "void",        Type::Void },
    { "int8",        Type::Int8 },
    { "int16",       Type::Int16 },
    { "int32",       Type::Int32 },
    { "int64",       Type::Int64 },
    { "bool",        Type::UInt8 },
    { "uint8",       Type::UInt8 },
    { "uint16",      Type::UInt16 },
    { "uint32",      Type::UInt32 },
    { "uint64",      Type::UInt64 },
    { "float32",     Type::Float32 },
    { "float64",     Type::Float64 },
    { "string",      Type::String },
    { "fixed_string", Type::FixedString },
    { "datetime",    Type::DateTime },
    { "datetime64",  Type::DateTime64 },
    { "date",        Type::Date },
    { "date32",      Type::Date32 },
    { "array",       Type::Array },
    { "nullable",    Type::Nullable },
    { "tuple",       Type::Tuple },
    { "enum8",       Type::Enum8 },
    { "enum16",      Type::Enum16 },
    { "uuid",        Type::UUID },
    { "ipv4",        Type::IPv4 },
    { "ipv6",        Type::IPv6 },
    { "int128",      Type::Int128 },
//    { "uint128",      Type::UInt128 },
    { "decimal",     Type::Decimal },
    { "decimal32",   Type::Decimal32 },
    { "decimal64",   Type::Decimal64 },
    { "decimal128",  Type::Decimal128 },
    { "lowcardinality", Type::LowCardinality },
    { "map",         Type::Map },
    { "point",       Type::Point },
    { "ring",        Type::Ring },
    { "polygon",     Type::Polygon },
    { "multipolygon", Type::MultiPolygon },
};

template <typename L, typename R>
inline int CompateStringsCaseInsensitive(const L& left, const R& right) {
    int64_t size_diff = left.size() - right.size();
    if (size_diff != 0)
        return size_diff > 0 ? 1 : -1;

#if defined _win_
    return _strnicmp(left.data(), right.data(), left.size());
#else
    return strncasecmp(left.data(), right.data(), left.size());
#endif
}

static Type::Code GetTypeCode(const std::string& name) {
    auto it = kTypeCode.find(name);
    if (it != kTypeCode.end()) {
        return it->second;
    }

    return Type::Void;
}

static TypeAst::Meta GetTypeMeta(const StringView& name) {
    if (name == "array") {
        return TypeAst::Array;
    }

    if (name == "null") {
        return TypeAst::Null;
    }

    if (name == "nullable") {
        return TypeAst::Nullable;
    }

    if (name == "tuple") {
        return TypeAst::Tuple;
    }

    if (name == "enum8" || name == "enum16") {
        return TypeAst::Enum;
    }

    if (name == "lowcardinality") {
        return TypeAst::LowCardinality;
    }

    if (name == "simpleaggregatefunction") {
        return TypeAst::SimpleAggregateFunction;
    }

    if (name == "map") {
        return TypeAst::Map;
    }

    return TypeAst::Terminal;
}

bool ValidateAST(const TypeAst& ast) {
    // Void terminal that is not actually "void" produced when unknown type is encountered.
    if (ast.meta == TypeAst::Terminal
            && ast.code == Type::Void
            && CompateStringsCaseInsensitive(ast.name, std::string_view("void")) != 0)
        //throw UnimplementedError("Unsupported type: " + ast.name);
        return false;

    return true;
}


TypeParser::TypeParser(const StringView& name)
    : cur_(name.data())
    , end_(name.data() + name.size())
    , type_(nullptr)
{
}

TypeParser::~TypeParser() = default;

bool TypeParser::Parse(TypeAst* type) {
    type_ = type;
    open_elements_.push(type_);

    size_t processed_tokens = 0;
    do {
        const Token & token = NextToken();
        switch (token.type) {
            case Token::QuotedString:
            {
                type_->meta = TypeAst::Terminal;
                if (token.value.length() < 1)
                    type_->value_string = {};
                else
                    type_->value_string = token.value.substr(1, token.value.length() - 2).to_string();
                type_->code = Type::String;
                break;
            }
            case Token::Name:
                type_->meta = GetTypeMeta(token.value);
                type_->name = token.value.to_string();
                type_->code = GetTypeCode(type_->name);
                break;
            case Token::Number:
                type_->meta = TypeAst::Number;
                type_->value = std::stol(token.value.to_string());
                break;
            case Token::String:
                type_->meta = TypeAst::String;
                type_->value_string = std::string(token.value);
                break;
            case Token::LPar:
                type_->elements.emplace_back(TypeAst());
                open_elements_.push(type_);
                type_ = &type_->elements.back();
                break;
            case Token::RPar:
                type_ = open_elements_.top();
                open_elements_.pop();
                break;
            case Token::Assign:
            case Token::Comma:
                type_ = open_elements_.top();
                open_elements_.pop();
                type_->elements.emplace_back(TypeAst());
                open_elements_.push(type_);
                type_ = &type_->elements.back();
                break;
            case Token::EOS:
            {
                // Ubalanced braces, brackets, etc is an error.
                if (open_elements_.size() != 1)
                    return false;

                // Empty input string, no tokens produced
                if (processed_tokens == 0)
                    return false;

                return ValidateAST(*type);
            }
            case Token::Invalid:
                return false;
        }
        ++processed_tokens;
    } while (true);
}

TypeParser::Token TypeParser::NextToken() {
    for (; cur_ < end_; ++cur_) {
        switch (*cur_) {
            case ' ':
            case '\n':
            case '\t':
            case '\0':
                continue;
            case '=':
                return Token{Token::Assign, StringView(cur_++, 1)};
            case '(':
                return Token{Token::LPar, StringView(cur_++, 1)};
            case ')':
                return Token{Token::RPar, StringView(cur_++, 1)};
            case ',':
                return Token{Token::Comma, StringView(cur_++, 1)};
            case '\'':
            {
                const auto end_quote_length = 1;
                const StringView end_quote{cur_, end_quote_length};
                // Fast forward to the closing quote.
                const auto start = cur_++;
                for (; cur_ < end_ - end_quote_length; ++cur_) {
                    // TODO (nemkov): handle escaping ?
                    if (end_quote == StringView{cur_, end_quote_length}) {
                        cur_ += end_quote_length;

                        return Token{Token::QuotedString, StringView{start, cur_}};
                    }
                }
                return Token{Token::QuotedString, StringView(cur_++, 1)};
            }

            default: {
                const char* st = cur_;

                if (*cur_ == '\'') {
                    for (st = ++cur_; cur_ < end_; ++cur_) {
                        if (*cur_ == '\'') {
                            return Token{Token::String, StringView(st, cur_++ - st)};
                        }
                    }

                    return Token{Token::Invalid, StringView()};
                }

                if (isalpha(*cur_) || *cur_ == '_') {
                    for (; cur_ < end_; ++cur_) {
                        if (!isalpha(*cur_) && !isdigit(*cur_) && *cur_ != '_') {
                            break;
                        }
                    }

                    return Token{Token::Name, StringView(st, cur_)};
                }

                if (isdigit(*cur_) || *cur_ == '-') {
                    for (++cur_; cur_ < end_; ++cur_) {
                        if (!isdigit(*cur_)) {
                            break;
                        }
                    }

                    return Token{Token::Number, StringView(st, cur_)};
                }

                return Token{Token::Invalid, StringView()};
            }
        }
    }

    return Token{Token::EOS, StringView()};
}


const TypeAst* ParseTypeName(const std::string& type_name) {
    // Cache for type_name.
    // Usually we won't have too many type names in the cache, so do not try to
    // limit cache size.
    static std::map<std::string, TypeAst> ast_cache;
    static std::mutex lock;

    std::lock_guard<std::mutex> guard(lock);
    auto it = ast_cache.find(type_name);
    if (it != ast_cache.end()) {
        return &it->second;
    }

    auto& ast = ast_cache[type_name];
    if (TypeParser(type_name).Parse(&ast)) {
        return &ast;
    }
    ast_cache.erase(type_name);
    return nullptr;
}

}
