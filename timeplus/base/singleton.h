#pragma once

namespace timeplus {

template <typename T>
T* Singleton() {
    static T instance;
    return &instance;
}

}
