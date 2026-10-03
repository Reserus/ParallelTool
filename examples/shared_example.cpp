#include "task_context.hpp"
#include <iostream>
#include <vector>

int main() {
    TaskContext ctx;
    ctx.set("numbers", std::vector<double>{1, 2, 3, 4});
    auto numbers = ctx.get_shared<std::vector<double>>("numbers");
    double sum = 0;
    for (double x : *numbers) sum += x;
    std::cout << "sum = " << sum << '\n';

    auto copy = ctx.get_copy<std::vector<double>>("numbers");
    copy[0] = 100;
    ctx.erase("numbers");
    std::cout << "snapshot first = " << numbers->front() << '\n';
}
