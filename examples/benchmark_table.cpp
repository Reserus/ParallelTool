#include "executor.hpp"
#include "threads_driver.hpp"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <stdexcept>
#include <sstream>
#include <utility>
#include <iostream>
#include <numeric>
#include <string>
#include <thread>
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
    const double median = (samples.size() % 2 == 0)
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
                const auto numbers =
                    ctx.get_shared<std::vector<double>>("numbers");

                double sum = 0.0;
                for (std::size_t i = begin; i < end; ++i) {
                    sum += (*numbers)[i];
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
                ctx.get_shared<std::vector<double>>("stage_" + std::to_string(stages));

            const double sum =
                std::accumulate(values->begin(), values->end(), 0.0);
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

BenchmarkResult run_low_exchange_benchmark(std::size_t n, std::size_t parts, int warmup_repeats, int measured_repeats) {
    const auto data = make_data(n);

    BenchmarkResult result;

    result.manual = measure_stats([&]() {
        result.manual_value = manual_parallel_sum(data, parts);
    }, warmup_repeats, measured_repeats);

    result.framework = measure_stats([&]() {
        result.framework_value = framework_parallel_sum(data, parts);
    }, warmup_repeats, measured_repeats);

    return result;
}

BenchmarkResult run_high_exchange_benchmark(std::size_t n, int stages, int warmup_repeats, int measured_repeats) {
    const auto data = make_data(n);

    BenchmarkResult result;

    result.manual = measure_stats([&]() {
        result.manual_value = manual_pipeline(data, stages);
    }, warmup_repeats, measured_repeats);

    result.framework = measure_stats([&]() {
        result.framework_value = framework_pipeline(data, stages);
    }, warmup_repeats, measured_repeats);

    return result;
}

void print_table_header(const std::string& title) {
    std::cout << "\n" << title << "\n";
    std::cout << std::string(104, '-') << "\n";
    std::cout
        << std::left
        << std::setw(12) << "N"
        << std::setw(24) << "Manual ms (med +/- sd)"
        << std::setw(24) << "Framework ms (med +/- sd)"
        << std::setw(16) << "Overhead ms"
        << std::setw(12) << "Slowdown"
        << std::setw(10) << "Diff"
        << "\n";
    std::cout << std::string(104, '-') << "\n";
}

void print_table_row(std::size_t n, const BenchmarkResult& r) {
    const double overhead_ms = r.framework.median_ms - r.manual.median_ms;
    const double slowdown = r.framework.median_ms / r.manual.median_ms;
    const double diff = std::abs(r.framework_value - r.manual_value);

    std::ostringstream manual_stats;
    manual_stats << std::fixed << std::setprecision(3)
                 << r.manual.median_ms << " +/- " << r.manual.stddev_ms;

    std::ostringstream framework_stats;
    framework_stats << std::fixed << std::setprecision(3)
                    << r.framework.median_ms << " +/- " << r.framework.stddev_ms;

    std::cout
        << std::left
        << std::setw(12) << n
        << std::setw(23) << manual_stats.str()
        << std::setw(23) << framework_stats.str()
        << std::setw(15) << overhead_ms
        << std::setw(12) << slowdown
        << std::setw(10) << diff
        << "\n";
}

void print_mean_summary(
    const std::string& label,
    const std::vector<std::pair<std::size_t, BenchmarkResult>>& results
) {
    std::cout << "\n" << label << " — arithmetic mean (ms)\n";
    std::cout << std::string(54, '-') << "\n";
    std::cout << std::left
              << std::setw(12) << "N"
              << std::setw(20) << "Manual mean"
              << std::setw(22) << "Framework mean"
              << "\n";
    std::cout << std::string(54, '-') << "\n";

    for (const auto& [n, result] : results) {
        std::cout << std::left
                  << std::setw(12) << n
                  << std::setw(20) << result.manual.mean_ms
                  << std::setw(22) << result.framework.mean_ms
                  << "\n";
    }
}

int main() {
    std::cout << std::fixed << std::setprecision(3);

    const std::size_t thread_count =
        std::max<std::size_t>(1, std::thread::hardware_concurrency());

    const int warmup_repeats = 3;
    const int measured_repeats = 21;

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

    std::vector<std::pair<std::size_t, BenchmarkResult>> low_results;
    print_table_header("LOW EXCHANGE: parallel sum by chunks");
    for (std::size_t n : low_exchange_sizes) {
        auto result = run_low_exchange_benchmark(
            n, thread_count, warmup_repeats, measured_repeats);
        print_table_row(n, result);
        low_results.emplace_back(n, std::move(result));
    }
    print_mean_summary("LOW EXCHANGE", low_results);

    const int stages = 8;

    std::vector<std::pair<std::size_t, BenchmarkResult>> high_results;
    print_table_header("HIGH EXCHANGE: pipeline with vector passing");
    for (std::size_t n : high_exchange_sizes) {
        auto result = run_high_exchange_benchmark(
            n, stages, warmup_repeats, measured_repeats);
        print_table_row(n, result);
        high_results.emplace_back(n, std::move(result));
    }
    print_mean_summary("HIGH EXCHANGE", high_results);

    std::cout << "\nThreads used: " << thread_count << "\n";
    std::cout << "Warmup repeats: " << warmup_repeats << "\n";
    std::cout << "Measured repeats: " << measured_repeats << "\n";
    std::cout << "Pipeline stages: " << stages << "\n";

    return 0;
}