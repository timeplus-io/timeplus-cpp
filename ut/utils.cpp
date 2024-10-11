#include "utils.h"

#include <timeplus/block.h>
#include <timeplus/client.h>
#include <timeplus/columns/column.h>
#include <timeplus/columns/array.h>
#include <timeplus/columns/date.h>
#include <timeplus/columns/decimal.h>
#include <timeplus/columns/enum.h>
#include <timeplus/columns/geo.h>
#include <timeplus/columns/ip4.h>
#include <timeplus/columns/ip6.h>
#include <timeplus/columns/numeric.h>
#include <timeplus/columns/map.h>
#include <timeplus/columns/string.h>
#include <timeplus/columns/tuple.h>
#include <timeplus/columns/uuid.h>
#include <timeplus/columns/nullable.h>

#include <timeplus/base/socket.h> // for ipv4-ipv6 platform-specific stuff

#include <cinttypes>
#include <iomanip>
#include <sstream>
#include <type_traits>


namespace {
using namespace timeplus;
std::ostream & printColumnValue(const ColumnRef& c, const size_t row, std::ostream & ostr);

struct DateTimeValue {
    explicit DateTimeValue(const time_t & v)
        : value(v)
    {}

    template <typename T>
    explicit DateTimeValue(const T & v)
        : value(v)
    {}

    const time_t value;
};

std::ostream& operator<<(std::ostream & ostr, const DateTimeValue & time) {
    const auto t = std::localtime(&time.value);
    char buffer[] = "2015-05-18 07:40:12\0\0";
    std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S", t);

    return ostr << buffer;
}

template <typename ColumnType, typename AsType = decltype(std::declval<ColumnType>().At(0)) >
bool doPrintValue(const ColumnRef & c, const size_t row, std::ostream & ostr) {
    if (const auto & casted_c = c->As<ColumnType>()) {
        if constexpr (is_container_v<std::decay_t<AsType>> && !std::is_same_v<ColumnType, ColumnString> &&
                      !std::is_same_v<ColumnType, ColumnFixedString> && !std::is_same_v<ColumnType, ColumnLowCardinalityT<ColumnString>> &&
                      !std::is_same_v<ColumnType, ColumnLowCardinalityT<ColumnFixedString>>) {
            ostr << PrintContainer{static_cast<AsType>(casted_c->At(row))};
        } else if constexpr (std::is_same_v<ColumnType, ColumnFixedString>) {
            ostr << static_cast<std::string_view>(casted_c->At(row).data()).substr(0, casted_c->FixedSize());
        } else if constexpr (std::is_same_v<ColumnType, ColumnLowCardinalityT<ColumnFixedString>>) {
            ostr << static_cast<std::string_view>(casted_c->At(row).data()).substr(0, casted_c->At(row).size());
        } else {
            ostr << static_cast<AsType>(casted_c->At(row));
        }
        return true;
    }
    return false;
}

template <typename ColumnEnumType>
bool doPrintEnumValue(const ColumnRef & c, const size_t row, std::ostream & ostr) {
    if (const auto & casted_c = c->As<ColumnEnumType>()) {
        // via temporary stream to preserve fill and alignment of the ostr
        std::stringstream sstr;
        sstr << casted_c->NameAt(row) << " (" << static_cast<int64_t>(casted_c->At(row)) << ")";
        ostr << sstr.str();
        return true;
    }
    return false;
}

template <>
bool doPrintValue<ColumnEnum8>(const ColumnRef & c, const size_t row, std::ostream & ostr) {
    return doPrintEnumValue<ColumnEnum8>(c, row, ostr);
}

template <>
bool doPrintValue<ColumnEnum16>(const ColumnRef & c, const size_t row, std::ostream & ostr) {
    return doPrintEnumValue<ColumnEnum16>(c, row, ostr);
}

template <>
bool doPrintValue<ColumnArray, void>(const ColumnRef & c, const size_t row, std::ostream & ostr) {
    // via temporary stream to preserve fill and alignment of the ostr
    std::stringstream sstr;

    if (const auto & array_col = c->As<ColumnArray>()) {
        const auto & row_values = array_col->GetAsColumn(row);
        sstr << "[";
        for (size_t i = 0; i < row_values->Size(); ++i) {
            printColumnValue(row_values, i, sstr);

            if (i < row_values->Size() - 1)
                sstr << ", ";
        }
        sstr << "]";
        ostr << sstr.str();

        return true;
    }
    return false;
}

template <>
bool doPrintValue<ColumnTuple, void>(const ColumnRef & c, const size_t row, std::ostream & ostr) {
    if (const auto & tupple_col = c->As<ColumnTuple>()) {
        std::stringstream sstr;
        sstr << "(";
        for (size_t i = 0; i < tupple_col->TupleSize(); ++i) {
            const auto & nested_col = (*tupple_col)[i];
            printColumnValue(nested_col, row, sstr);
            if (i < tupple_col->TupleSize() - 1)
                sstr << ", ";
        }
        sstr << ")";
        ostr << sstr.str();
        return true;
    }
    return false;
}

template <>
bool doPrintValue<ColumnUUID, void>(const ColumnRef & c, const size_t row, std::ostream & ostr) {
    if (const auto & uuid_col = c->As<ColumnUUID>()) {
        ostr << ToString(uuid_col->At(row));
        return true;
    }
    return false;
}

template <>
bool doPrintValue<ColumnMap, void>(const ColumnRef & c, const size_t row, std::ostream & ostr) {
    // via temporary stream to preserve fill and alignment of the ostr
    std::stringstream sstr;
    if (const auto & map_col = c->As<ColumnMap>()) {
        sstr << "{";
        const auto tuples = map_col->GetAsColumn(row);
        for (size_t i = 0; i < tuples->Size(); ++i) {
            printColumnValue(tuples, i, sstr);
            if (i < tuples->Size() - 1)
                sstr << ", ";
        }

        sstr << "}";
        ostr << sstr.str();
        return true;
    }
    return false;
}

template <>
bool doPrintValue<ColumnNullable, void>(const ColumnRef& c, const size_t row, std::ostream& ostr) {
    if (const auto& col = c->As<ColumnNullable>()) {
        if (col->IsNull(row)) {
            ostr << "null";
        } else {
            printColumnValue(col->Nested(), row, ostr);
        }
        return true;
    }
    return false;
}

std::ostream & printColumnValue(const ColumnRef& c, const size_t row, std::ostream & ostr) {
    const auto r = false
        || doPrintValue<ColumnString>(c, row, ostr)
        || doPrintValue<ColumnFixedString>(c, row, ostr)
        || doPrintValue<ColumnUInt8, unsigned int>(c, row, ostr)
        || doPrintValue<ColumnUInt32>(c, row, ostr)
        || doPrintValue<ColumnUInt16>(c, row, ostr)
        || doPrintValue<ColumnUInt64>(c, row, ostr)
        || doPrintValue<ColumnInt8, int>(c, row, ostr)
        || doPrintValue<ColumnInt32>(c, row, ostr)
        || doPrintValue<ColumnInt16>(c, row, ostr)
        || doPrintValue<ColumnInt64>(c, row, ostr)
        || doPrintValue<ColumnInt128>(c, row, ostr)
        || doPrintValue<ColumnInt256>(c, row, ostr)
        || doPrintValue<ColumnFloat32>(c, row, ostr)
        || doPrintValue<ColumnFloat64>(c, row, ostr)
        || doPrintValue<ColumnEnum8>(c, row, ostr)
        || doPrintValue<ColumnEnum16>(c, row, ostr)
        || doPrintValue<ColumnDate, DateTimeValue>(c, row, ostr)
        || doPrintValue<ColumnDateTime, DateTimeValue>(c, row, ostr)
        || doPrintValue<ColumnDateTime64, DateTimeValue>(c, row, ostr)
        || doPrintValue<ColumnDecimal>(c, row, ostr)
        || doPrintValue<ColumnIPv4>(c, row, ostr)
        || doPrintValue<ColumnIPv6>(c, row, ostr)
        || doPrintValue<ColumnArray, void>(c, row, ostr)
        || doPrintValue<ColumnTuple, void>(c, row, ostr)
        || doPrintValue<ColumnUUID, void>(c, row, ostr)
        || doPrintValue<ColumnMap, void>(c, row, ostr)
        || doPrintValue<ColumnPoint>(c, row, ostr)
        || doPrintValue<ColumnRing>(c, row, ostr)
        || doPrintValue<ColumnPolygon>(c, row, ostr)
        || doPrintValue<ColumnMultiPolygon>(c, row, ostr)
        || doPrintValue<ColumnLowCardinalityT<ColumnString>>(c, row, ostr)
        || doPrintValue<ColumnLowCardinalityT<ColumnFixedString>>(c, row, ostr)
        || doPrintValue<ColumnNullable, void>(c, row, ostr);
    if (!r)
        ostr << "Unable to print value of type " << c->GetType().GetName();

    return ostr;
}

struct ColumnValue {
    const ColumnRef& c;
    size_t row;
};

std::ostream & operator<<(std::ostream & ostr, const ColumnValue& v) {
    return printColumnValue(v.c, v.row, ostr);
}

}

std::ostream& operator<<(std::ostream & ostr, const PrettyPrintBlock & pretty_print_block) {
    // Pretty-print block:
    // - names of each column
    // - types of each column
    // - values of column row-by-row

    const auto & block = pretty_print_block.block;
    if (block.GetRowCount() == 0 || block.GetColumnCount() == 0)
        return ostr;

    std::vector<size_t> column_width(block.GetColumnCount());
    const auto horizontal_bar = '|';
    const auto cross = '+';
    const auto vertical_bar = '-';

    std::stringstream sstr;
    for (auto i = block.begin(); i != block.end(); ++i) {
        auto width = column_width[i.ColumnIndex()] = std::max(i.Type()->GetName().size(), i.Name().size());
        for (size_t j = 0; j < block.GetRowCount(); ++j) {
            std::stringstream tmp_stream;
            printColumnValue(i.Column(), j, tmp_stream);
            width = column_width[i.ColumnIndex()] = std::max(width, tmp_stream.str().size());
        }
        sstr << cross << std::setw(width + 2) << std::setfill(vertical_bar) << vertical_bar;
    }
    sstr << cross;
    const std::string split_line(sstr.str());

    ostr << split_line << std::endl;
    // column name
    for (auto i = block.begin(); i != block.end(); ++i) {
        auto width = column_width[i.ColumnIndex()];
        ostr << horizontal_bar << ' ' << std::setw(width) << i.Name() << ' ';
    }
    ostr << horizontal_bar << std::endl;;
    ostr << split_line << std::endl;

    // column type
    for (auto i = block.begin(); i != block.end(); ++i) {
        auto width = column_width[i.ColumnIndex()];
        ostr << horizontal_bar << ' ' << std::setw(width) << i.Type()->GetName() << ' ';
    }
    ostr << horizontal_bar << std::endl;
    ostr << split_line << std::endl;

    // values
    for (size_t row_index = 0; row_index < block.GetRowCount(); ++row_index) {
        for (auto i = block.begin(); i != block.end(); ++i) {
            auto width = column_width[i.ColumnIndex()];
            ostr << horizontal_bar << ' ' << std::setw(width) << ColumnValue{i.Column(), row_index} << ' ';
        }
        ostr << horizontal_bar << std::endl;
    }
    ostr << split_line << std::endl;

    return ostr;
}

std::ostream& operator<<(std::ostream& ostr, const in_addr& addr) {
    char buf[INET_ADDRSTRLEN];
    const char* ip_str = inet_ntop(AF_INET, &addr, buf, sizeof(buf));

    if (!ip_str)
        return ostr << "<!INVALID IPv4 VALUE!>";

    return ostr << ip_str;
}

std::ostream& operator<<(std::ostream& ostr, const in6_addr& addr) {
    char buf[INET6_ADDRSTRLEN];
    const char* ip_str = inet_ntop(AF_INET6, &addr, buf, sizeof(buf));

    if (!ip_str)
        return ostr << "<!INVALID IPv6 VALUE!>";

    return ostr << ip_str;
}

namespace timeplus {

std::ostream& operator<<(std::ostream & ostr, const Block & block) {
    if (block.GetRowCount() == 0 || block.GetColumnCount() == 0)
        return ostr;

    for (size_t col = 0; col < block.GetColumnCount(); ++col) {
        const auto & c = block[col];
        ostr << c->GetType().GetName() << " [";

        for (size_t row = 0; row < block.GetRowCount(); ++row) {
            printColumnValue(c, row, ostr);
            if (row != block.GetRowCount() - 1)
                ostr << ", ";
        }
        ostr << "]";

        if (col != block.GetColumnCount() - 1)
            ostr << "\n";
    }

    return ostr;
}

std::ostream& operator<<(std::ostream & ostr, const Type & type) {
    return ostr << type.GetName();
}

std::ostream & operator<<(std::ostream & ostr, const ServerInfo & server_info) {
    return ostr << server_info.name << "/" << server_info.display_name
                << " ver "
                << server_info.version_major << "."
                << server_info.version_minor << "."
                << server_info.version_patch
                << " (" << server_info.revision << ")";
}

std::ostream & operator<<(std::ostream & ostr, const Profile & profile) {
    return ostr
        << "rows : " << profile.rows
        << " blocks : " << profile.blocks
        << " bytes : " << profile.bytes
        << " rows_before_limit : " << profile.rows_before_limit
        << " applied_limit : " << profile.applied_limit
        << " calculated_rows_before_limit : " << profile.calculated_rows_before_limit;
}

std::ostream & operator<<(std::ostream & ostr, const Progress & progress) {
    return ostr
        << "rows : " << progress.rows
        << " bytes : " << progress.bytes
        << " total_rows : " << progress.total_rows
        << " written_rows : " << progress.written_rows
        << " written_bytes : " << progress.written_bytes;
}

}

uint64_t versionNumber(const ServerInfo & server_info) {
    return versionNumber(server_info.version_major, server_info.version_minor, server_info.version_patch, server_info.revision);
}

std::string ToString(const timeplus::UUID& v) {
    std::string result(36, 0);
    // ffff ff ff ss ssssss
    const int count = std::snprintf(result.data(), result.size() + 1, "%.8" PRIx64 "-%.4" PRIx64 "-%.4" PRIx64 "-%.4" PRIx64 "-%.12" PRIx64,
                                    v.items[0] >> 32, (v.items[0] >> 16) & 0xffff, v.items[0] & 0xffff, v.items[1] >> 48, v.items[1] & 0xffffffffffff);
    if (count != 36) {
        throw std::runtime_error("Error while converting UUID to string");
    }
    return result;
}

// Return the current time unit based on the specified precision.When precision is set to 0, return seconds.
// eg: for 123456789 nanoseconds:
// if precision = 3 => return 123 (milliseconds)
// if precision = 6 => return 123456 (microseconds)
// if precision = 9 => return 123456789 (nanoseconds)

uint64_t getCurrentTimeByPrecision(int precision) {
    auto now = std::chrono::high_resolution_clock::now();
    auto duration = now.time_since_epoch();

    uint64_t current_time_nanoseconds = std::chrono::duration_cast<std::chrono::nanoseconds>(duration).count();

    uint64_t divisor = 1;
    for (int i = precision; i < 9; ++i) {
        divisor *= 10;
    }
    return current_time_nanoseconds / divisor;
}

std::string formatTimestamp(uint64_t timestamp, int precision) {

    auto seconds = timestamp;

    uint64_t multip = 1;
    
    if(precision > 0){
        for (int i = 0; i < precision; ++i) {
            multip *=10;
        }
    }
    seconds /= multip;

    auto remaining_nanoseconds = timestamp % multip;

    std::time_t time_t_seconds = static_cast<std::time_t>(seconds);
    std::tm* tm_time = std::localtime(&time_t_seconds);
    std::ostringstream oss;
    oss << std::put_time(tm_time, "%Y-%m-%d %H:%M:%S");

    if (precision > 0 && precision <= 9) {
        oss << "." << std::setfill('0');
        uint64_t fraction = remaining_nanoseconds;
        oss << std::setw(precision) << std::setfill('0') << fraction;
    }
    oss<<" "<< std::fixed;

    return oss.str();
}


std::string formatTimestamp(uint64_t timestamp){
    std::time_t time_t_seconds = static_cast<std::time_t>(timestamp);
    std::tm* tm_time = std::localtime(&time_t_seconds);
    std::ostringstream oss;
    oss << std::put_time(tm_time, "%Y-%m-%d");
    oss<<" "<< std::fixed;

    return oss.str();
}
