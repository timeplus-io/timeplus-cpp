#pragma once

#include "column.h"

namespace timeplus {

struct CreateColumnByTypeSettings
{
    bool low_cardinality_as_wrapped_column = false;
};

ColumnRef CreateColumnByType(const std::string& type_name, CreateColumnByTypeSettings settings = {});

}
