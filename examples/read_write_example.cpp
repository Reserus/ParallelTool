#include "executor.hpp"
#include "threads_driver.hpp"

#include <iostream>
#include <stdexcept>
#include <vector>

int main() {
    TaskGraph graph;
    TaskContext context;

    const auto load = graph.add_task(
        "load",
        TaskAccessSpec{{}, {"numbers"}, {}},
        [](TaskAccess& data) {
            auto& numbers = data.write<std::vector<int>>("numbers");
            numbers = {1, 2, 3, 4};
        }
    );

    const auto double_numbers = graph.add_task(
        "double_numbers",
        TaskAccessSpec{{}, {}, {{"numbers", "doubled"}}},
        [](TaskAccess& data) {
            auto& doubled = data.read_write<std::vector<int>>("numbers", "doubled");
            for (int& value : doubled) {
                value *= 2;
            }
        }
    );

    const auto sum = graph.add_task(
        "sum",
        TaskAccessSpec{{"doubled"}, {"sum"}, {}},
        [](TaskAccess& data) {
            const auto& doubled = data.read<std::vector<int>>("doubled");
            auto& result = data.write<int>("sum");
            for (int value : doubled) {
                result += value;
            }
        }
    );

    const auto failing = graph.add_task(
        "failing_update",
        TaskAccessSpec{{}, {}, {{"doubled", "broken"}}},
        [](TaskAccess& data) {
            auto& broken = data.read_write<std::vector<int>>("doubled", "broken");
            broken.push_back(999);
            throw std::runtime_error("intentional failure after modifying staging");
        }
    );

    graph.add_dependency(load, double_numbers);
    graph.add_dependency(double_numbers, sum);
    graph.add_dependency(double_numbers, failing);

    ThreadsDriver driver(4);
    Executor executor(driver);

    try {
        executor.run(graph, context);
        std::cout << "[BUG] execution finished without exception\n";
        return 1;
    } catch (const std::exception& error) {
        std::cout << "[EXPECTED] caught exception: " << error.what() << '\n';
    }

    const auto numbers = context.get_copy<std::vector<int>>("numbers");
    const auto doubled = context.get_copy<std::vector<int>>("doubled");

    if (numbers != std::vector<int>{1, 2, 3, 4}) {
        std::cerr << "[BUG] source value was modified\n";
        return 1;
    }
    if (doubled != std::vector<int>{2, 4, 6, 8}) {
        std::cerr << "[BUG] read_write result is incorrect\n";
        return 1;
    }
    if (context.contains("broken")) {
        std::cerr << "[BUG] failed output was published\n";
        return 1;
    }
    if (context.contains("sum")) {
        std::cout << "[INFO] successful sibling output sum = "
                  << context.get_copy<int>("sum") << '\n';
    }

    std::cout << "[OK] read/write staging and rollback work\n";
    return 0;
}
