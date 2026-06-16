#include "executor.hpp"
#include "threads_driver.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <string>
#include <thread>
#include <vector>

using Clock = std::chrono::high_resolution_clock;

struct BenchmarkResult {
    double manual_ms = 0.0;
    double framework_ms = 0.0;
    double manual_value = 0.0;
    double framework_value = 0.0;
};

template <class Func>
double measure_average_ms(Func&& func, int repeats) {
    double total_ms = 0.0;

    for (int i = 0; i < repeats; ++i) {
        const auto start = Clock::now();
        func();
        const auto end = Clock::now();
        total_ms += std::chrono::duration<double, std::milli>(end - start).count();
    }

    return total_ms / static_cast<double>(repeats);
}

std::vector<double> make_data(std::size_t n) {
    std::vector<double> data(n);

    for (std::size_t i = 0; i < n; ++i) {
        data[i] = 1.0 + static_cast<double>(i % 1000) * 0.001;
    }

    return data;
}

double manual_parallel_sum(const std::vector<double>& data, std::size_t parts) {
    if (parts == 0) {
        parts = 1;
    }

    std::vector<std::thread> workers;
    std::vector<double> partial(parts, 0.0);

    const std::size_t n = data.size();
    const std::size_t block = (n + parts - 1) / parts;

    for (std::size_t part = 0; part < parts; ++part) {
        const std::size_t begin = part * block;
        const std::size_t end = std::min(n, begin + block);

        workers.emplace_back([&, part, begin, end]() {
            double sum = 0.0;
            for (std::size_t i = begin; i < end; ++i) {
                sum += data[i];
            }
            partial[part] = sum;
        });
    }

    for (auto& t : workers) {
        t.join();
    }

    return std::accumulate(partial.begin(), partial.end(), 0.0);
}

double framework_parallel_sum(const std::vector<double>& data, std::size_t parts) {
    TaskGraph graph;
    TaskContext context;

    context.set("numbers", data);

    std::vector<TaskGraph::TaskId> chunk_ids;
    chunk_ids.reserve(parts);

    const std::size_t n = data.size();
    const std::size_t block = (n + parts - 1) / parts;

    for (std::size_t part = 0; part < parts; ++part) {
        const std::size_t begin = part * block;
        const std::size_t end = std::min(n, begin + block);
        const std::string out_key = "partial_" + std::to_string(part);

        auto id = graph.add_task(
            "chunk_sum_" + std::to_string(part),
            {"numbers"},
            {out_key},
            [begin, end, out_key](TaskContext& ctx) {
                const auto numbers = ctx.get_copy<std::vector<double>>("numbers");

                double sum = 0.0;
                for (std::size_t i = begin; i < end; ++i) {
                    sum += numbers[i];
                }

                ctx.set(out_key, sum);
            }
        );

        chunk_ids.push_back(id);
    }

    std::vector<std::string> reduce_inputs;
    reduce_inputs.reserve(parts);
    for (std::size_t part = 0; part < parts; ++part) {
        reduce_inputs.push_back("partial_" + std::to_string(part));
    }

    auto reduce_id = graph.add_task(
        "reduce_sum",
        reduce_inputs,
        {"sum"},
        [parts](TaskContext& ctx) {
            double sum = 0.0;
            for (std::size_t part = 0; part < parts; ++part) {
                sum += ctx.get_copy<double>("partial_" + std::to_string(part));
            }
            ctx.set("sum", sum);
        }
    );

    for (auto id : chunk_ids) {
        graph.add_dependency(id, reduce_id);
    }

    ThreadsDriver driver(parts);
    Executor executor(driver);
    executor.run(graph, context);

    return context.get_copy<double>("sum");
}

double manual_pipeline(std::vector<double> data, int stages) {
    for (int stage = 0; stage < stages; ++stage) {
        const double mul = 1.0 + 0.01 * static_cast<double>(stage + 1);
        const double add = 0.1 * static_cast<double>(stage + 1);

        for (double& x : data) {
            x = x * mul + add;
        }
    }

    return std::accumulate(data.begin(), data.end(), 0.0);
}

double framework_pipeline(const std::vector<double>& data, int stages) {
    TaskGraph graph;
    TaskContext context;

    context.set("stage_0", data);

    std::vector<TaskGraph::TaskId> stage_ids;
    stage_ids.reserve(static_cast<std::size_t>(stages));

    for (int stage = 0; stage < stages; ++stage) {
        const std::string in_key = "stage_" + std::to_string(stage);
        const std::string out_key = "stage_" + std::to_string(stage + 1);

        auto id = graph.add_task(
            "pipeline_stage_" + std::to_string(stage),
            {in_key},
            {out_key},
            [stage, in_key, out_key](TaskContext& ctx) {
                auto values = ctx.get_copy<std::vector<double>>(in_key);

                const double mul = 1.0 + 0.01 * static_cast<double>(stage + 1);
                const double add = 0.1 * static_cast<double>(stage + 1);

                for (double& x : values) {
                    x = x * mul + add;
                }

                ctx.set(out_key, std::move(values));
            }
        );

        stage_ids.push_back(id);
    }

    for (int stage = 1; stage < stages; ++stage) {
        graph.add_dependency(stage_ids[stage - 1], stage_ids[stage]);
    }

    auto final_id = graph.add_task(
        "final_reduce",
        {"stage_" + std::to_string(stages)},
        {"result"},
        [stages](TaskContext& ctx) {
            const auto values =
                ctx.get_copy<std::vector<double>>("stage_" + std::to_string(stages));

            const double sum = std::accumulate(values.begin(), values.end(), 0.0);
            ctx.set("result", sum);
        }
    );

    if (!stage_ids.empty()) {
        graph.add_dependency(stage_ids.back(), final_id);
    }

    ThreadsDriver driver(std::max<std::size_t>(1, std::thread::hardware_concurrency()));
    Executor executor(driver);
    executor.run(graph, context);

    return context.get_copy<double>("result");
}

BenchmarkResult run_low_exchange_benchmark(std::size_t n, std::size_t parts, int repeats) {
    const auto data = make_data(n);

    BenchmarkResult result;

    result.manual_ms = measure_average_ms([&]() {
        result.manual_value = manual_parallel_sum(data, parts);
    }, repeats);

    result.framework_ms = measure_average_ms([&]() {
        result.framework_value = framework_parallel_sum(data, parts);
    }, repeats);

    return result;
}

BenchmarkResult run_high_exchange_benchmark(std::size_t n, int stages, int repeats) {
    const auto data = make_data(n);

    BenchmarkResult result;

    result.manual_ms = measure_average_ms([&]() {
        result.manual_value = manual_pipeline(data, stages);
    }, repeats);

    result.framework_ms = measure_average_ms([&]() {
        result.framework_value = framework_pipeline(data, stages);
    }, repeats);

    return result;
}

void print_table_header(const std::string& title) {
    std::cout << "\n=== " << title << " ===\n";
    std::cout
        << std::left
        << std::setw(12) << "N"
        << std::setw(14) << "Manual(ms)"
        << std::setw(16) << "Framework(ms)"
        << std::setw(14) << "Overhead(ms)"
        << std::setw(12) << "Slowdown"
        << std::setw(14) << "Diff"
        << "\n";

    std::cout << std::string(82, '-') << "\n";
}

void print_table_row(std::size_t n, const BenchmarkResult& r) {
    const double overhead_ms = r.framework_ms - r.manual_ms;
    const double slowdown = r.framework_ms / r.manual_ms;
    const double diff = std::abs(r.framework_value - r.manual_value);

    std::cout
        << std::left
        << std::setw(12) << n
        << std::setw(14) << r.manual_ms
        << std::setw(16) << r.framework_ms
        << std::setw(14) << overhead_ms
        << std::setw(12) << slowdown
        << std::setw(14) << diff
        << "\n";
}

int main() {
    std::cout << std::fixed << std::setprecision(3);

    const std::size_t thread_count =
        std::max<std::size_t>(1, std::thread::hardware_concurrency());

    const int repeats = 5;

    const std::vector<std::size_t> low_exchange_sizes = {
        100'000,
        500'000,
        1'000'000,
        2'000'000,
        5'000'000
    };

    const std::vector<std::size_t> high_exchange_sizes = {
        50'000,
        100'000,
        200'000,
        500'000,
        1'000'000
    };

    print_table_header("Low exchange workload (parallel sum by chunks)");
    for (std::size_t n : low_exchange_sizes) {
        const auto result = run_low_exchange_benchmark(n, thread_count, repeats);
        print_table_row(n, result);
    }

    const int stages = 8;

    print_table_header("High exchange workload (pipeline with vector passing)");
    for (std::size_t n : high_exchange_sizes) {
        const auto result = run_high_exchange_benchmark(n, stages, repeats);
        print_table_row(n, result);
    }

    std::cout << "\nThreads used: " << thread_count << "\n";
    std::cout << "Repeats: " << repeats << "\n";
    std::cout << "Pipeline stages: " << stages << "\n";

    return 0;
}