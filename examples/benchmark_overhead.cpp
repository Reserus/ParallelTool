#include "executor.hpp"
#include "threads_driver.hpp"

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

    TaskGraph::TaskId prev_id = static_cast<TaskGraph::TaskId>(-1);
    bool has_prev = false;

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

        if (has_prev) {
            graph.add_dependency(prev_id, id);
        }

        prev_id = id;
        has_prev = true;
    }

    auto final_id = graph.add_task(
        "final_reduce",
        {"stage_" + std::to_string(stages)},
        {"result"},
        [stages](TaskContext& ctx) {
            const auto values = ctx.get_copy<std::vector<double>>(
                "stage_" + std::to_string(stages)
            );

            const double sum = std::accumulate(values.begin(), values.end(), 0.0);
            ctx.set("result", sum);
        }
    );

    graph.add_dependency(prev_id, final_id);

    ThreadsDriver driver(std::thread::hardware_concurrency());
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

void print_result(const std::string& title, const BenchmarkResult& r) {
    const double slowdown = r.framework_ms / r.manual_ms;
    const double extra_ms = r.framework_ms - r.manual_ms;
    const double value_diff = std::abs(r.framework_value - r.manual_value);

    std::cout << "=== " << title << " ===\n";
    std::cout << "manual time      : " << r.manual_ms << " ms\n";
    std::cout << "framework time   : " << r.framework_ms << " ms\n";
    std::cout << "extra overhead   : " << extra_ms << " ms\n";
    std::cout << "slowdown         : " << slowdown << "x\n";
    std::cout << "manual result    : " << r.manual_value << "\n";
    std::cout << "framework result : " << r.framework_value << "\n";
    std::cout << "result diff      : " << value_diff << "\n\n";
}

int main() {
    std::cout << std::fixed << std::setprecision(3);

    const std::size_t thread_count =
        std::max<std::size_t>(1, std::thread::hardware_concurrency());

    const int repeats = 5;

    // 1) Минимальный обмен: одна большая структура данных,
    //    параллельная обработка по кускам, потом редукция.
    const auto low_exchange = run_low_exchange_benchmark(
        2'000'000,
        thread_count,
        repeats
    );

    // 2) Интенсивный обмен: несколько стадий, на каждой стадии
    //    передаётся новый vector<double>.
    const auto high_exchange = run_high_exchange_benchmark(
        500'000,
        8,
        repeats
    );

    print_result("Low exchange workload", low_exchange);
    print_result("High exchange workload", high_exchange);

    return 0;
}