#include "executor.hpp"
#include "threads_driver.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using Clock = std::chrono::steady_clock;

struct TimingStats {
    double mean_ms = 0.0;
    double median_ms = 0.0;
    double stddev_ms = 0.0;
};

struct BenchmarkResult {
    TimingStats manual;
    TimingStats framework;
    double manual_value = 0.0;
    double framework_value = 0.0;
};

template <class Func>
TimingStats measure_stats(Func&& func, int warmup_repeats, int measured_repeats) {
    if (measured_repeats <= 0) {
        throw std::invalid_argument("measured_repeats must be positive");
    }

    for (int i = 0; i < warmup_repeats; ++i) {
        func();
    }

    std::vector<double> samples;
    samples.reserve(static_cast<std::size_t>(measured_repeats));
    for (int i = 0; i < measured_repeats; ++i) {
        const auto start = Clock::now();
        func();
        const auto end = Clock::now();
        samples.push_back(std::chrono::duration<double, std::milli>(
            end - start).count());
    }

    const double mean = std::accumulate(samples.begin(), samples.end(), 0.0) /
                        static_cast<double>(samples.size());
    std::sort(samples.begin(), samples.end());
    const std::size_t middle = samples.size() / 2;
    const double median = samples.size() % 2 == 0
        ? (samples[middle - 1] + samples[middle]) / 2.0
        : samples[middle];

    double variance = 0.0;
    for (double sample : samples) {
        const double diff = sample - mean;
        variance += diff * diff;
    }
    variance /= static_cast<double>(samples.size());

    return {mean, median, std::sqrt(variance)};
}

std::vector<double> make_data(std::size_t n) {
    std::vector<double> data(n);
    for (std::size_t i = 0; i < n; ++i) {
        data[i] = 1.0 + static_cast<double>(i % 1000) * 0.001;
    }
    return data;
}

double manual_parallel_sum(const std::vector<double>& data, std::size_t parts) {
    parts = std::max<std::size_t>(1, parts);
    std::vector<std::thread> workers;
    std::vector<double> partial(parts, 0.0);
    const std::size_t block = (data.size() + parts - 1) / parts;

    for (std::size_t part = 0; part < parts; ++part) {
        const std::size_t begin = part * block;
        const std::size_t end = std::min(data.size(), begin + block);
        workers.emplace_back([&, part, begin, end]() {
            for (std::size_t i = begin; i < end; ++i) {
                partial[part] += data[i];
            }
        });
    }

    for (auto& worker : workers) {
        worker.join();
    }
    return std::accumulate(partial.begin(), partial.end(), 0.0);
}

double framework_parallel_sum(const std::vector<double>& data, std::size_t parts) {
    parts = std::max<std::size_t>(1, parts);
    TaskGraph graph;
    TaskContext context;
    context.set("numbers", data);

    std::vector<TaskGraph::TaskId> chunk_ids;
    const std::size_t block = (data.size() + parts - 1) / parts;

    for (std::size_t part = 0; part < parts; ++part) {
        const std::size_t begin = part * block;
        const std::size_t end = std::min(data.size(), begin + block);
        const std::string out_key = "partial_" + std::to_string(part);

        chunk_ids.push_back(graph.add_task(
            "chunk_sum_" + std::to_string(part),
            TaskAccessSpec{{"numbers"}, {out_key}, {}},
            [begin, end, out_key](TaskAccess& access) {
                const auto& numbers =
                    access.read<std::vector<double>>("numbers");
                auto& partial = access.write<double>(out_key);
                for (std::size_t i = begin; i < end; ++i) {
                    partial += numbers[i];
                }
            }
        ));
    }

    std::vector<std::string> reduce_inputs;
    reduce_inputs.reserve(parts);
    for (std::size_t part = 0; part < parts; ++part) {
        reduce_inputs.push_back("partial_" + std::to_string(part));
    }

    const auto reduce_id = graph.add_task(
        "reduce_sum",
        TaskAccessSpec{reduce_inputs, {"sum"}, {}},
        [parts](TaskAccess& access) {
            auto& result = access.write<double>("sum");
            for (std::size_t part = 0; part < parts; ++part) {
                result += access.read<double>("partial_" + std::to_string(part));
            }
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
        for (double& value : data) {
            value = value * mul + add;
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

        stage_ids.push_back(graph.add_task(
            "pipeline_stage_" + std::to_string(stage),
            TaskAccessSpec{{}, {}, {{in_key, out_key}}},
            [stage, in_key, out_key](TaskAccess& access) {
                auto& values = access.read_write<std::vector<double>>(
                    in_key, out_key);
                const double mul = 1.0 + 0.01 * static_cast<double>(stage + 1);
                const double add = 0.1 * static_cast<double>(stage + 1);
                for (double& value : values) {
                    value = value * mul + add;
                }
            }
        ));
    }

    for (std::size_t stage = 1; stage < stage_ids.size(); ++stage) {
        graph.add_dependency(stage_ids[stage - 1], stage_ids[stage]);
    }

    const std::string final_key = "stage_" + std::to_string(stages);
    const auto final_id = graph.add_task(
        "final_reduce",
        TaskAccessSpec{{final_key}, {"result"}, {}},
        [final_key](TaskAccess& access) {
            const auto& values = access.read<std::vector<double>>(final_key);
            auto& result = access.write<double>("result");
            result = std::accumulate(values.begin(), values.end(), 0.0);
        }
    );
    graph.add_dependency(stage_ids.back(), final_id);

    ThreadsDriver driver(std::max<std::size_t>(1, std::thread::hardware_concurrency()));
    Executor executor(driver);
    executor.run(graph, context);
    return context.get_copy<double>("result");
}

BenchmarkResult run_benchmark(
    const std::vector<double>& data,
    std::size_t parts,
    int stages,
    int warmup_repeats,
    int measured_repeats
) {
    BenchmarkResult result;
    result.manual = measure_stats([&]() {
        result.manual_value = stages == 0
            ? manual_parallel_sum(data, parts)
            : manual_pipeline(data, stages);
    }, warmup_repeats, measured_repeats);
    result.framework = measure_stats([&]() {
        result.framework_value = stages == 0
            ? framework_parallel_sum(data, parts)
            : framework_pipeline(data, stages);
    }, warmup_repeats, measured_repeats);
    return result;
}

void print_header(const std::string& title) {
    std::cout << "\n" << title << "\n"
              << std::string(104, '-') << "\n"
              << std::left << std::setw(12) << "N"
              << std::setw(24) << "Manual ms (med +/- sd)"
              << std::setw(24) << "TaskAccess ms (med +/- sd)"
              << std::setw(16) << "Overhead ms"
              << std::setw(12) << "Slowdown"
              << std::setw(10) << "Diff" << "\n"
              << std::string(104, '-') << "\n";
}

void print_row(std::size_t n, const BenchmarkResult& result) {
    const double overhead = result.framework.median_ms - result.manual.median_ms;
    const double slowdown = result.framework.median_ms / result.manual.median_ms;
    const double diff = std::abs(result.framework_value - result.manual_value);

    std::ostringstream manual;
    manual << std::fixed << std::setprecision(3)
           << result.manual.median_ms << " +/- " << result.manual.stddev_ms;
    std::ostringstream framework;
    framework << std::fixed << std::setprecision(3)
              << result.framework.median_ms << " +/- " << result.framework.stddev_ms;

    std::cout << std::left << std::setw(12) << n
              << std::setw(23) << manual.str()
              << std::setw(23) << framework.str()
              << std::setw(15) << overhead
              << std::setw(12) << slowdown
              << std::setw(10) << diff << "\n";
}

int main() {
    std::cout << std::fixed << std::setprecision(3);
    const std::size_t thread_count =
        std::max<std::size_t>(1, std::thread::hardware_concurrency());
    const int warmup_repeats = 3;
    const int measured_repeats = 21;

    const std::vector<std::size_t> low_exchange_sizes = {
        100'000, 500'000, 1'000'000, 2'000'000, 5'000'000
    };
    const std::vector<std::size_t> high_exchange_sizes = {
        50'000, 100'000, 200'000, 500'000, 1'000'000
    };
    const int stages = 8;

    print_header("LOW EXCHANGE: parallel sum by chunks with TaskAccess");
    for (std::size_t n : low_exchange_sizes) {
        print_row(n, run_benchmark(
            make_data(n), thread_count, 0, warmup_repeats, measured_repeats));
    }

    print_header("HIGH EXCHANGE: ReadWrite vector pipeline");
    for (std::size_t n : high_exchange_sizes) {
        print_row(n, run_benchmark(
            make_data(n), thread_count, stages, warmup_repeats, measured_repeats));
    }

    std::cout << "\nThreads used: " << thread_count << '\n'
              << "Warmup repeats: " << warmup_repeats << '\n'
              << "Measured repeats: " << measured_repeats << '\n'
              << "Pipeline stages: " << stages << '\n';
    return 0;
}
