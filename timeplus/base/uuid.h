#pragma once

#include <cstdint>
#include <utility>
#include "wide_integer.h"

namespace timeplus {

// using UInt128 = std::pair<uint64_t, uint64_t>;
using UInt128 = class wide::integer<128, unsigned int>;

using UUID = UInt128;

}
