#pragma once

#include "column.h"
#include "string.h"

#include <cstdint>
#include <string_view>

namespace timeplus {

/**
 * Represents column of JSON documents.
 * Leverages the same underlying wire format as strings until
 * native JSON serialization support is available server-side.
 */
class ColumnJson : public ColumnString {

};

}