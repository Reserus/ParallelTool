#pragma once

#include "task_context.hpp"

#include <cstddef>
#include <functional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

class TaskGraph {
public:
    using TaskId = std::size_t;
    using TaskFunction = std::function<void(TaskContext&)>;

    struct TaskNode {
        std::string name;
        std::vector<std::string> inputs;
        std::vector<std::string> outputs;
        TaskFunction func;
        std::vector<TaskId> next;
        std::vector<TaskId> prev;
    };

    TaskId add_task(std::string name,
                    std::vector<std::string> inputs,
                    std::vector<std::string> outputs,
                    TaskFunction func) {
        if (!func) {
            throw std::invalid_argument("TaskGraph: empty task function");
        }

        tasks_.push_back(TaskNode{
            std::move(name),
            std::move(inputs),
            std::move(outputs),
            std::move(func),
            {},
            {}
        });

        return tasks_.size() - 1;
    }

    void add_dependency(TaskId before, TaskId after) {
        check_task_id(before);
        check_task_id(after);

        tasks_[before].next.push_back(after);
        tasks_[after].prev.push_back(before);
    }

    const TaskNode& task(TaskId id) const {
        check_task_id(id);
        return tasks_[id];
    }

    TaskNode& task(TaskId id) {
        check_task_id(id);
        return tasks_[id];
    }

    std::size_t size() const {
        return tasks_.size();
    }

    bool empty() const {
        return tasks_.empty();
    }

private:
    void check_task_id(TaskId id) const {
        if (id >= tasks_.size()) {
            throw std::out_of_range("TaskGraph: invalid TaskId");
        }
    }

    std::vector<TaskNode> tasks_;
};