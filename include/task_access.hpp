#pragma once

#include "task_context.hpp"

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

struct TaskAccessSpec {
    std::vector<std::string> reads;
    std::vector<std::string> writes;
    std::vector<std::pair<std::string, std::string>> read_writes;
};

class TaskAccess {
public:
    TaskAccess(TaskContext& context, TaskAccessSpec spec)
        : context_(context), spec_(std::move(spec)) {
        for (const auto& key : spec_.reads) {
            readable_.insert(key);
        }
        for (const auto& key : spec_.writes) {
            writable_.insert(key);
        }
        for (const auto& access : spec_.read_writes) {
            readable_.insert(access.first);
            writable_.insert(access.second);
        }
    }

    template <class T>
    const T& read(const std::string& key) const {
        require_read(key);

        auto it = read_values_.find(key);
        if (it == read_values_.end()) {
            it = read_values_.emplace(key, context_.get_value_shared(key)).first;
        }

        const T* value = std::get_if<T>(it->second.get());
        if (!value) {
            throw std::runtime_error("TaskAccess: bad type for key: " + key);
        }
        return *value;
    }

    template <class T>
    T& write(const std::string& key) {
        require_write(key);
        auto [it, inserted] = staged_.try_emplace(key, T{});
        (void)inserted;

        T* value = std::get_if<T>(&it->second);
        if (!value) {
            throw std::runtime_error("TaskAccess: bad type for key: " + key);
        }
        return *value;
    }

    template <class T>
    T& read_write(const std::string& input, const std::string& output) {
        require_read_write(input, output);
        auto it = staged_.find(output);
        if (it == staged_.end()) {
            it = staged_.emplace(output, context_.get_value_copy(input)).first;
        }

        T* value = std::get_if<T>(&it->second);
        if (!value) {
            throw std::runtime_error("TaskAccess: bad type for key: " + output);
        }
        return *value;
    }

private:
    friend class ThreadsDriver;

    void commit() {
        context_.publish(std::move(staged_));
    }

    void require_read(const std::string& key) const {
        if (!readable_.contains(key)) {
            throw std::runtime_error("TaskAccess: undeclared read: " + key);
        }
    }

    void require_write(const std::string& key) const {
        if (!writable_.contains(key)) {
            throw std::runtime_error("TaskAccess: undeclared write: " + key);
        }
    }

    void require_read_write(const std::string& input, const std::string& output) const {
        for (const auto& access : spec_.read_writes) {
            if (access.first == input && access.second == output) {
                return;
            }
        }
        throw std::runtime_error("TaskAccess: undeclared read/write: " + input + " -> " + output);
    }

    TaskContext& context_;
    TaskAccessSpec spec_;
    std::unordered_set<std::string> readable_;
    std::unordered_set<std::string> writable_;
    mutable std::unordered_map<std::string, std::shared_ptr<const TaskContext::Value>> read_values_;
    std::unordered_map<std::string, TaskContext::Value> staged_;
};
