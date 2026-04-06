#pragma once

#include "task_context.hpp"
#include "task_graph.hpp"

class IExecutionDriver {
public:
    virtual ~IExecutionDriver() = default;
    virtual void run(TaskGraph& graph, TaskContext& context) = 0;
};