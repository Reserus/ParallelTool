#include "mpi_driver.hpp"

#include <stdexcept>

MpiDriver::MpiDriver() = default;
MpiDriver::~MpiDriver() = default;

void MpiDriver::run(TaskGraph& graph, TaskContext& context) {
    (void)graph;
    (void)context;

    throw std::runtime_error(
        "Todo"
    );
}