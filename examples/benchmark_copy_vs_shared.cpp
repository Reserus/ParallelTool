#include "task_context.hpp"
#include <algorithm>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <numeric>
#include <stdexcept>
#include <vector>

using Clock = std::chrono::steady_clock;
struct Measurement { double ms; double sum; };

Measurement measure(const TaskContext& ctx, bool shared) {
    const auto start = Clock::now();
    double sum;
    if (shared) {
        auto data = ctx.get_shared<std::vector<double>>("numbers");
        sum = std::accumulate(data->begin(), data->end(), 0.0);
    } else {
        auto data = ctx.get_copy<std::vector<double>>("numbers");
        sum = std::accumulate(data.begin(), data.end(), 0.0);
    }
    return {std::chrono::duration<double, std::milli>(Clock::now()-start).count(), sum};
}

double median(std::vector<double> values) {
    std::sort(values.begin(), values.end());
    return values[values.size()/2];
}

int main() {
    constexpr int repeats = 21;
    std::cout << "Read + full sum; median of " << repeats << " runs\n"
              << "N,copy_ms,shared_ms,speedup\n" << std::fixed << std::setprecision(6);
    for (std::size_t n : {1000u, 100000u, 1000000u, 5000000u}) {
        TaskContext ctx;
        std::vector<double> data(n, 1.0);
        ctx.set("numbers", std::move(data));
        auto check = [n](Measurement m) {
            if (!std::isfinite(m.sum) || m.sum != static_cast<double>(n))
                throw std::runtime_error("Wrong sum");
        };
        for (int i=0; i<3; ++i) {
            check(measure(ctx, false));
            check(measure(ctx, true));
        }
        std::vector<double> copies, shared;
        for (int i=0; i<repeats; ++i) {
            for (int j=0; j<2; ++j) {
                bool use_shared = ((i+j)%2 != 0);
                auto m = measure(ctx, use_shared);
                check(m);
                (use_shared ? shared : copies).push_back(m.ms);
            }
        }
        const double a=median(copies), b=median(shared);
        std::cout << n << ',' << a << ',' << b << ',' << (b>0 ? a/b : 0) << '\n';
    }
}
