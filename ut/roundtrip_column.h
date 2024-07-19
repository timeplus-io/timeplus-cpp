#pragma once

#include <timeplus/columns/column.h>
#include <memory>

namespace timeplus {
    class Client;
}

timeplus::ColumnRef RoundtripColumnValues(timeplus::Client& client, timeplus::ColumnRef expected);

template <typename T>
auto RoundtripColumnValuesTyped(timeplus::Client& client, std::shared_ptr<T> expected_col)
{
    return RoundtripColumnValues(client, expected_col)->template As<T>();
}
