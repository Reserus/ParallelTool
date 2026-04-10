#include "executor.hpp"
#include "threads_driver.hpp"

#include <chrono>
#include <exception>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

int main() {
    TaskGraph graph;
    TaskContext context;

    auto load = graph.add_task(
        "load_numbers",
        {},
        {"numbers"},
        [](TaskContext& ctx) {
            ctx.set("numbers", std::vector<int>{1, 2, 3, 4, 5});
            std::cout << "[OK] load_numbers\n";
        }
    );

    auto compute_sum = graph.add_task(
        "compute_sum",
        {"numbers"},
        {"sum"},
        [](TaskContext& ctx) {
            auto numbers = ctx.get_copy<std::vector<int>>("numbers");

            int sum = 0;
            for (int x : numbers) {
                sum += x;
            }

            ctx.set("sum", sum);
            std::cout << "[OK] compute_sum\n";
        }
    );

    auto compute_fail = graph.add_task(
        "compute_fail",
        {"numbers"},
        {"broken_value"},
        [](TaskContext& ctx) {
            auto numbers = ctx.get_copy<std::vector<int>>("numbers");
            (void)numbers;

            std::cout << "[FAIL] compute_fail is going to throw\n";
            throw std::runtime_error("compute_fail failed intentionally");
        }
    );

    auto print_sum = graph.add_task(
        "print_sum",
        {"sum"},
        {},
        [](TaskContext& ctx) {
            int sum = ctx.get_copy<int>("sum");
            std::cout << "[OK] print_sum: sum = " << sum << "\n";
        }
    );

    auto should_not_run = graph.add_task(
        "should_not_run",
        {"broken_value"},
        {},
        [](TaskContext& ctx) {
            std::cout << "[BUG] should_not_run was executed\n";

            if (ctx.contains("broken_value")) {
                std::cout << "[BUG] broken_value exists, but task should not have started\n";
            }

            throw std::runtime_error("should_not_run must never execute");
        }
    );

    auto independent = graph.add_task(
        "independent_task",
        {},
        {"independent_result"},
        [](TaskContext& ctx) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            ctx.set("independent_result", std::string("done"));
            std::cout << "[OK] independent_task\n";
        }
    );

    graph.add_dependency(load, compute_sum);
    graph.add_dependency(load, compute_fail);
    graph.add_dependency(compute_sum, print_sum);
    graph.add_dependency(compute_fail, should_not_run);

    ThreadsDriver driver(4);
    Executor executor(driver);

    try {
        executor.run(graph, context);
        std::cout << "[BUG] execution finished without exception\n";
    } catch (const std::exception& e) {
        std::cout << "[EXPECTED] caught exception: " << e.what() << "\n";
    }

    std::cout << "\n=== POST CHECK ===\n";

    if (context.contains("sum")) {
        std::cout << "[INFO] sum exists\n";
    } else {
        std::cout << "[INFO] sum does not exist\n";
    }

    if (context.contains("broken_value")) {
        std::cout << "[BUG] broken_value exists\n";
    } else {
        std::cout << "[OK] broken_value does not exist\n";
    }

    if (context.contains("independent_result")) {
        std::cout << "[INFO] independent_result exists\n";
    } else {
        std::cout << "[INFO] independent_result does not exist\n";
    }

    return 0;
}