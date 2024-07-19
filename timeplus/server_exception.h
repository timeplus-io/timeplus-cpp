#pragma once

#include <string>
#include <memory>

namespace timeplus {
struct Exception {
    int code = 0;
    std::string name;
    std::string display_text;
    std::string stack_trace;
    /// Pointer to nested exception.
    std::unique_ptr<Exception> nested;
};

}
