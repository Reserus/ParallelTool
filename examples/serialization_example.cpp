#include "task_context.hpp"
#include "task_value_serialization.hpp"

#include <iostream>
#include <vector>

int main() {
    TaskContext::Value original = std::vector<int>{10, 20, 30, 40};

    auto bytes = task_serialization::serialize_value(original);
    auto restored = task_serialization::deserialize_value(bytes);

    if (auto* vec = std::get_if<std::vector<int>>(&restored)) {
        std::cout << "Restored vector<int>: ";
        for (int x : *vec) {
            std::cout << x << " ";
        }
        std::cout << "\n";
    } else {
        std::cout << "Wrong type restored\n";
    }

    return 0;
}