#pragma once

#include "execution_driver.hpp"

class MpiDriver final : public IExecutionDriver {
public:
    MpiDriver();
    ~MpiDriver() override;

    MpiDriver(const MpiDriver&) = delete;
    MpiDriver& operator=(const MpiDriver&) = delete;

    void run(TaskGraph& graph, TaskContext& context) override;
};