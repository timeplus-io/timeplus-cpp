#pragma once

#include <any>
#include <sstream>
#include <string>
#include <unordered_map>
#include "column.h"

namespace timeplus {

/// Escaped:
/// 1) x.y 		-> 	`x.y`
/// 2) `x.y`.a 	-> 	`\`x.y\`.a`
/// 3) `x.y``.a	-> 	`\`x.y\`\`.a` (special case)
/// No Escape:
/// 4) x 		-> 	x
/// 5) `x.y` 	-> 	`x.y`
/// 6) `x.y`a	-> 	`x.y`a
std::string EscapeJsonPath(const std::string&);

/// Unescaped:
/// 1) `x.y` 		-> 	x.y
/// 2) `\`x.y\`.a`	-> 	`x.y`.a
/// No unescape:
/// 3) x 		-> 	x
/// 4) `x.y`.a	-> 	`x.y`.a
/// 5) \`x.y\`.a	-> 	\`x.y\`.a
/// 6) `x.y`.a`	-> 	`x.y`.a` (special case)
/// 7) ``		-> 	`` (special case)
std::string UnescapeJsonPath(const std::string&);

/// Split the Json path to sub-paths For example:
/// 1) "id" 		-> 	["id"]
/// 2) "id.a" 	-> 	["id", "a"]
/// 3) "`x.y`.z" ->  ["x.y", "z"]
/// ... Others: unknown behavior
std::vector<std::string> SplitJsonPath(const std::string&);

// Split the sub-paths to a whole path For example:
// 1) ["id"] 		-> 	"id"
// 2) ["id", "a"] 	-> 	"id.a"
// 3) ["x.y", "z"] 	-> 	"`x.y`.z"
// ... Others: unknown behavior
std::string BuildJsonPath(const std::vector<std::string>&);

using JsonKey = std::string;
using JsonValue = ColumnRef;

/// Json type only support (u)int(8/16/32/128/256), float(32/64), bool, string, array types.
class ColumnJson : public Column {
    /// Data is stored in leaf node, every leaf node is a column of Json fundamental
    /// type (i.e. (u)int(8/16/32/128/256), float(32/64), bool, string, array).
    /// for example:
    //  '{"id": 1, "obj": { "x": "abc", "y": 2}}', the elems is:
    // <"id", []int32>,
    // <"obj.x", []string>,
    // <"obj.y", []int32>
public:
    /// Constructing ColumnJson from a map object
    template <class Map, class K = typename Map::key_type, class V = typename Map::mapped_type,
              class = std::enable_if_t<std::conjunction_v<std::is_same<K, JsonKey>, std::disjunction<std::is_same<V, JsonValue>>>>>
    explicit ColumnJson(const std::vector<Map>& jsons) : Column(Type::CreateJson()) {
        for (auto& json : jsons) {
            Append(json);
        }
    }

    /// Append a sequences elements to column.
    template <class Map, class K = typename Map::key_type, class V = typename Map::mapped_type,
              class = std::enable_if_t<std::conjunction_v<std::is_same<K, JsonKey>, std::disjunction<std::is_same<V, JsonValue>>>>>
    void Append(const Map& json) {
        auto sz = Size();
        for (const auto& [key, value] : json) {
            if (data_.find(key) == data_.end()) {
                data_[key] = value->CloneEmpty();
                AppendZeroValueToColumn(data_[key], sz);
            }
            data_[key]->Append(value);
        }

        for (const auto& [key, value] : data_) {
            if (json.find(key) == json.end()) {
                AppendZeroValueToColumn(value, std::begin(json)->second->Size());
            }
        }
    }

    /// Get the value corresponding to the key.
    /// If path is invaild or not exists , it will throw a error.
    JsonValue& operator[](const JsonKey& path);
    JsonValue& At(const JsonKey& path);

public:
    ColumnJson() : Column(Type::CreateJson()) {}

    /// Appends content of given column to the end of current one.
    void Append(ColumnRef column) override;

    /// Increase the capacity of the column for large block insertion.
    void Reserve(size_t new_cap) override;

    /// Loads column prefix from input stream.
    bool LoadPrefix(InputStream* input, size_t rows) override;

    /// Loads column data from input stream.
    bool LoadBody(InputStream* input, size_t rows) override;

    /// Saves column prefix to output stream.
    void SavePrefix(OutputStream* output) override;

    /// Saves column data to output stream.
    void SaveBody(OutputStream* output) override;

    /// Clear column data .
    void Clear() override;

    /// Returns count of rows in the column.
    size_t Size() const override;

    /// Makes slice of the current column.
    ColumnRef Slice(size_t begin, size_t len) const override;
    ColumnRef CloneEmpty() const override;
    void Swap(Column& other) override;

private:
    /// get a nested json from a (path,value) json.
    std::shared_ptr<ColumnJson> NestedJson() const;

    /// Get full type string from a nested Json.
    /// Only ColumnJson from NestedJson can call this function.
    /// for example:
    /// {"a": 1, "b": {"c": 2, "d": 3}, "e": [1,2,3,4]}
    /// -> "tuple(a int,b tuple(c int, d int),e array(int))"
    std::string NestedJsonType();

    size_t GetFullComplexType(std::string_view);

    std::string GetSubObjectName(std::string_view);
    std::string GetSubObjectType(std::string_view);

    /// deserialisation a path + value Json from a full type string.
    bool DeserialisationJson(std::string_view type_with_name, std::vector<std::string>& now_path, InputStream* input, size_t rows);

    /// serialisation a path + value Json by a full type string.
    void SerialisationJson(std::string_view type_with_name, OutputStream* output);

    /// append %rows empty values to %col.
    void AppendZeroValueToColumn(ColumnRef col, size_t rows);

    std::unordered_map<std::string, JsonValue> data_;
};
}  // namespace timeplus