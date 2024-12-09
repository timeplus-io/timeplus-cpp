#include "json.h"
#include "../base/wire_format.h"
#include "../exceptions.h"
#include "array.h"
#include "factory.h"
#include "numeric.h"
#include "string.h"

namespace timeplus {

std::string EscapeJsonPath(const std::string& path) {
    bool has_escape = false;
    bool has_dot = false;
    for (auto& ch : path) {
        if (ch == '.') {
            has_dot = true;
            if (!has_escape) {
                break;
            }
            continue;
        } else if (ch == '`') {
            if (has_escape) {
                has_dot = false;
            }
            has_escape = !has_escape;
        }
    }

    if (!has_dot) {
        return path;
    }

    std::string res;
    res.push_back('`');
    for (auto& ch : path) {
        if (ch == '`') {
            res.push_back('\\');
        }
        res.push_back(ch);
    }
    res.push_back('`');
    return res;
}

std::string UnescapeJsonPath(const std::string& path) {
    if (path.size() <= 2 || path[0] != '`' || path.back() != '`') {
        return path;
    }
    std::string res;
    for (size_t i = 1; i < path.size() - 1; ++i) {
        if (path[i] == '`') {
            return path;
        }
        if (path.substr(i, 2) == "\\`") {
            i++;
        }
        res.push_back(path[i]);
    }
    return res;
}

std::vector<std::string> SplitJsonPath(const std::string& path) {
    std::vector<std::string> parts;
    std::string part;
    bool has_escape = false;
    for (auto& ch : path) {
        if (ch == '`') {
            has_escape = !has_escape;
        }
        if (ch == '.' && !has_escape) {
            parts.push_back(UnescapeJsonPath(part));
            part.clear();
            continue;
        }
        part.push_back(ch);
    }

    // last part
    parts.push_back(UnescapeJsonPath(part));
    return parts;
}

std::string BuildJsonPath(const std::vector<std::string>& parts) {
    std::string path;
    for (size_t i = 0; i < parts.size(); ++i) {
        auto escapeed_part = EscapeJsonPath(parts[i]);
        path.append(escapeed_part);
        if (i != parts.size() - 1) {
            path.push_back('.');
        }
    }
    return path;
}

JsonValue& ColumnJson::operator[](const JsonKey& path) {
    auto it = data_.find(path);
    if (it == data_.end()) {
        throw ValidationError("path is invaild or not exists.");
    }
    return it->second;
}

JsonValue& ColumnJson::At(const JsonKey& path) {
    return (*this)[path];
}

void ColumnJson::Append(ColumnRef column) {
    auto sz = Size();
    if (auto col = column->As<ColumnJson>()) {
        for (auto& [key, value] : col->data_) {
            if (data_.find(key) == data_.end()) {
                data_[key] = value->CloneEmpty();
                AppendZeroValueToColumn(data_[key], sz);
            }
            data_[key]->Append(value);
        }

        for (auto& [key, value] : data_) {
            if (col->data_.find(key) == col->data_.end()) {
                AppendZeroValueToColumn(value, col->Size());
            }
        }
    }
}

void ColumnJson::Reserve(size_t new_cap) {
    for (auto& col : data_) {
        col.second->Reserve(new_cap);
    }
}

bool ColumnJson::LoadPrefix(InputStream* input, [[maybe_unused]] size_t rows) {
    uint64_t kind;
    if (!WireFormat::ReadUInt64(*input, &kind)) {
        return false;
    }
    return true;
}

bool ColumnJson::LoadBody(InputStream* input, size_t rows) {
    std::string type_with_name;
    if (!WireFormat::ReadString(*input, &type_with_name)) {
        return false;
    }
    std::vector<std::string> now_path;
    return DeserialisationJson(type_with_name, now_path, input, rows);
}

void ColumnJson::SavePrefix(OutputStream* output) {
    // serialize Json as tuple.
    WireFormat::WriteUInt64(*output, 0ull);
}

void ColumnJson::SaveBody(OutputStream* output) {
    auto nested_json = NestedJson();
    std::string type_with_name = nested_json->NestedJsonType();
    WireFormat::WriteString(*output, type_with_name);
    nested_json->SerialisationJson(type_with_name, output);
}

void ColumnJson::Clear() {
    data_.clear();
}

size_t ColumnJson::Size() const {
    if (data_.empty()) {
        return 0ul;
    }
    return data_.begin()->second->Size();
}

ColumnRef ColumnJson::Slice(size_t begin, size_t len) const {
    if (begin + len > Size()) throw ValidationError("Slice indexes are out of bounds");

    auto col = std::dynamic_pointer_cast<ColumnJson>(CloneEmpty());
    for (auto& [key, value] : data_) {
        col->data_[key] = value->Slice(begin, len);
    }
    return col;
}

ColumnRef ColumnJson::CloneEmpty() const {
    return std::make_shared<ColumnJson>();
}

void ColumnJson::Swap(Column& other) {
    ColumnJson& col = dynamic_cast<ColumnJson&>(other);
    data_.swap(col.data_);
}

std::shared_ptr<ColumnJson> ColumnJson::NestedJson() const {
    auto rt = std::make_shared<ColumnJson>();
    std::shared_ptr<ColumnJson> cur;
    for (auto& [key, value] : data_) {
        cur = rt;
        auto parts = SplitJsonPath(key);
        for (size_t i = 0; i < parts.size(); ++i) {
            parts[i] = EscapeJsonPath(parts[i]);
            if (i == parts.size() - 1) {
                cur->data_[parts[i]] = value;
                break;
            }
            if (cur->data_.find(parts[i]) == cur->data_.end()) {
                cur->data_[parts[i]] = CloneEmpty();
            }
            cur = cur->data_[parts[i]]->As<ColumnJson>();
            if (cur == nullptr) {
                throw ValidationError("same json path with different value type.");
            }
        }
    }
    return rt;
}

std::string ColumnJson::NestedJsonType() {
    std::string res;
    res.append("tuple(");
    size_t cnt = 0;
    for (auto& [key, val] : data_) {
        res.append(key + " ");
        if (auto col = std::dynamic_pointer_cast<ColumnJson>(val); col != nullptr) {
            res.append(col->NestedJsonType());
        } else {
            res.append(val->GetType().GetName());
        }
        cnt++;
        if (cnt != data_.size()) {
            res.append(", ");
        }
    }
    res.append(")");
    return res;
}

void ColumnJson::AppendZeroValueToColumn(ColumnRef col, size_t rows) {
    while (rows--) {
        switch (col->Type()->GetCode()) {
            case Type::Code::Int8: {
                auto c = std::dynamic_pointer_cast<ColumnInt8>(col);
                c->Append(static_cast<int8_t>(0));
                break;
            }
            case Type::Code::Int16: {
                auto c = std::dynamic_pointer_cast<ColumnInt16>(col);
                c->Append(static_cast<int16_t>(0));
                break;
            }
            case Type::Code::Int32: {
                auto c = std::dynamic_pointer_cast<ColumnInt32>(col);
                c->Append(static_cast<int32_t>(0));
                break;
            }
            case Type::Code::Int64: {
                auto c = std::dynamic_pointer_cast<ColumnInt64>(col);
                c->Append(static_cast<int64_t>(0));
                break;
            }
            case Type::Code::UInt8: {
                auto c = std::dynamic_pointer_cast<ColumnUInt8>(col);
                c->Append(static_cast<uint8_t>(0));
                break;
            }
            case Type::Code::UInt16: {
                auto c = std::dynamic_pointer_cast<ColumnUInt16>(col);
                c->Append(static_cast<uint16_t>(0));
                break;
            }
            case Type::Code::UInt32: {
                auto c = std::dynamic_pointer_cast<ColumnUInt32>(col);
                c->Append(static_cast<uint32_t>(0));
                break;
            }
            case Type::Code::UInt64: {
                auto c = std::dynamic_pointer_cast<ColumnUInt64>(col);
                c->Append(static_cast<uint64_t>(0));
                break;
            }
            case Type::Code::Float32: {
                auto c = std::dynamic_pointer_cast<ColumnFloat32>(col);
                c->Append(0.0f);
                break;
            }
            case Type::Code::Float64: {
                auto c = std::dynamic_pointer_cast<ColumnFloat64>(col);
                c->Append(0.0);
                break;
            }
            case Type::Code::Array: {
                auto c = std::dynamic_pointer_cast<ColumnArray>(col);
                c->AppendAsColumn(std::make_shared<ColumnUInt8>());
                break;
            }
            case Type::Code::String: {
                auto c = std::dynamic_pointer_cast<ColumnString>(col);
                c->Append("");
                break;
            }
            default:
                throw ValidationError("json doesn't suppot type " + col->Type()->GetName());
        }
    }
}

size_t ColumnJson::GetFullComplexType(std::string_view type_with_name) {
    int st = 1;
    size_t len = 0;
    for (auto& ch : type_with_name) {
        switch (ch) {
            case '(':
                st++;
                break;
            case ')':
                st--;
                break;
        }
        len++;
        if (st == 0) {
            break;
        }
    }
    return len;
}

std::string ColumnJson::GetSubObjectName(std::string_view type_with_name) {
    bool has_escape = false;
    size_t name_len = 0;
    for (; name_len < type_with_name.size(); name_len++) {
        if (type_with_name[name_len] == '`') {
            has_escape = !has_escape;
        }
        if (type_with_name[name_len] == ' ' && !has_escape) {
            break;
        }
    }
    return std::string{type_with_name.substr(0, name_len)};
}

std::string ColumnJson::GetSubObjectType(std::string_view type_with_name) {
    size_t type_len = 0;
    /// sub object.
    if (type_with_name.substr(0, 5) == "tuple" || type_with_name.substr(0, 5) == "array") {
        type_len += 6;
        type_len += GetFullComplexType(type_with_name.substr(6 /* skip "tuple(" or "array(" */));
        return std::string{type_with_name.substr(0, type_len)};
    }

    /// terminal type.
    for (; type_len < type_with_name.size(); type_len++) {
        if (type_with_name[type_len] == ')' || type_with_name[type_len] == ',') {
            break;
        }
    }
    return std::string{type_with_name.substr(0, type_len)};
}

bool ColumnJson::DeserialisationJson(std::string_view type_with_name, std::vector<std::string>& now_path, InputStream* input, size_t rows) {
    size_t cur = 6; /* skip "tuple(" */
    for (; cur < type_with_name.size() - 1 /* without ")" */; cur += 2 /* skip ", " */) {
        std::string name = GetSubObjectName(type_with_name.substr(cur));
        cur += name.size() + 1 /* skip " " */;
        if (cur >= type_with_name.size()) return false;
        now_path.push_back(name);

        std::string type = GetSubObjectType(type_with_name.substr(cur));
        if (type.substr(0, 5) == "tuple") {
            /// Deserialisation sub object.
            if (!DeserialisationJson(type, now_path, input, rows)) {
                return false;
            }
        } else {
            /// Deserialisation terminal type.
            auto path = BuildJsonPath(now_path);
            data_[path] = CreateColumnByType(type);
            if (!data_[path]->Load(input, rows)) {
                return false;
            }
        }
        now_path.pop_back();
        cur += type.size();
    }
    return true;
}

void ColumnJson::SerialisationJson(std::string_view type_with_name, OutputStream* output) {
    size_t cur = 6; /* skip "tuple(" */
    for (; cur < type_with_name.size() - 1 /* without ")" */; cur += 2 /* skip ", " */) {
        std::string name = GetSubObjectName(type_with_name.substr(cur));
        cur += name.size() + 1 /* skip " " */;
        if (cur >= type_with_name.size()) throw ValidationError("serialisation type " + std::string{type_with_name} + " failed.");

        std::string type = GetSubObjectType(type_with_name.substr(cur));
        if (type.substr(0, 5) == "tuple") {
            /// Serialisation sub object.
            auto sub_col = std::dynamic_pointer_cast<ColumnJson>(data_[name]);
            sub_col->SerialisationJson(type, output);
            cur += type.size();
            continue;
        } else {
            /// Serialisation terminal type.
            data_[name]->Save(output);
        }
        cur += type.size();
    }
}

}  // namespace timeplus
