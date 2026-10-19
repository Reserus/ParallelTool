#pragma once

#include "task_context.hpp"
#include "task_access.hpp"

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
    using AccessTaskFunction = std::function<void(TaskAccess&)>;

    struct TaskNode {
        std::string name;
        std::vector<std::string> inputs;
        std::vector<std::string> outputs;
        TaskFunction func;
        AccessTaskFunction access_func;
        TaskAccessSpec access_spec;
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
            {},
            {},
            {}
        });

        return tasks_.size() - 1;
    }

    TaskId add_task(std::string name,
                    TaskAccessSpec access_spec,
                    AccessTaskFunction func) {
        if (!func) {
            throw std::invalid_argument("TaskGraph: empty task function");
        }

        std::vector<std::string> inputs = access_spec.reads;
        std::vector<std::string> outputs = access_spec.writes;
        for (const auto& access : access_spec.read_writes) {
            inputs.push_back(access.first);
            outputs.push_back(access.second);
        }

        tasks_.push_back(TaskNode{
            std::move(name),
            std::move(inputs),
            std::move(outputs),
            {},
            std::move(func),
            std::move(access_spec),
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
