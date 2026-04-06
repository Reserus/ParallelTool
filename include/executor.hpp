#pragma once

#include "execution_driver.hpp"

class Executor {
    IExecutionDriver& driver_;
public:
    explicit Executor(IExecutionDriver& driver)
        : driver_(driver) {}

    void run(TaskGraph& graph, TaskContext& context) {
        driver_.run(graph, context);
    }

};