#include "executor.hpp"
#include "threads_driver.hpp"
// #include "mpi_driver.hpp"

#include <iostream>
#include <vector>

int main() {
    TaskGraph graph;
    TaskContext context;

    auto load = graph.add_task(
        "load_numbers",
        {},
        {"numbers"},
        [](TaskContext& ctx) {
            ctx.set("numbers", std::vector<int>{7, 2, 9, 1, 5, 3});
            std::cout << "load_numbers\n";
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
            std::cout << "compute_sum\n";
        }
    );

    auto compute_max = graph.add_task(
        "compute_max",
        {"numbers"},
        {"max"},
        [](TaskContext& ctx) {
            auto numbers = ctx.get_copy<std::vector<int>>("numbers");

            int mx = numbers[0];
            for (int x : numbers) {
                if (x > mx) {
                    mx = x;
                }
            }

            ctx.set("max", mx);
            std::cout << "compute_max\n";
        }
    );

    auto print = graph.add_task(
        "print_result",
        {"sum", "max"},
        {},
        [](TaskContext& ctx) {
            int sum = ctx.get_copy<int>("sum");
            int mx = ctx.get_copy<int>("max");

            std::cout << "print_result\n";
            std::cout << "sum = " << sum << "\n";
            std::cout << "max = " << mx << "\n";
        }
    );

    graph.add_dependency(load, compute_sum);
    graph.add_dependency(load, compute_max);
    graph.add_dependency(compute_sum, print);
    graph.add_dependency(compute_max, print);

    ThreadsDriver driver(4);
    // MpiDriver driver;

    Executor executor(driver);

    try {
        executor.run(graph, context);
        std::cout << "All tasks completed\n";
    } catch (const std::exception& e) {
        std::cout << "Execution failed: " << e.what() << "\n";
    }

    return 0;
}