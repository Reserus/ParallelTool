#include "executor.hpp"
#include "threads_driver.hpp"

#include <exception>
#include <iostream>
#include <stdexcept>

int main() {
    TaskGraph graph;
    TaskContext context;

    auto root_fail = graph.add_task(
        "root_fail",
        {},
        {"x"},
        [](TaskContext& ctx) {
            (void)ctx;
            std::cout << "[FAIL] root_fail\n";
            throw std::runtime_error("root task failed");
        }
    );

    auto child = graph.add_task(
        "child",
        {"x"},
        {},
        [](TaskContext& ctx) {
            (void)ctx;
            std::cout << "[BUG] child executed\n";
        }
    );

    graph.add_dependency(root_fail, child);

    ThreadsDriver driver(2);
    Executor executor(driver);

    try {
        executor.run(graph, context);
        std::cout << "[BUG] no exception\n";
    } catch (const std::exception& e) {
        std::cout << "[EXPECTED] caught exception: " << e.what() << "\n";
    }

    return 0;
}