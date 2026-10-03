#pragma once

#include <memory>
#include <mutex>
#include <utility>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <variant>
#include <vector>

class TaskContext {
public:
    using Value = std::variant<
        int,
        double,
        std::string,
        std::vector<int>,
        std::vector<double>
    >;

private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::shared_ptr<const Value>> data_;

public:
    TaskContext() = default;

    template <class T>
    void set(const std::string& key, T value) {
        static_assert(
            std::is_same_v<T, int> ||
            std::is_same_v<T, double> ||
            std::is_same_v<T, std::string> ||
            std::is_same_v<T, std::vector<int>> ||
            std::is_same_v<T, std::vector<double>>,
            "TaskContext::set: unsupported type"
        );

        auto stored = std::make_shared<const Value>(std::move(value));
        std::lock_guard<std::mutex> lock(mutex_);
        data_[key] = std::move(stored);
    }

    template <class T>
    T get_copy(const std::string& key) const {
        static_assert(
            std::is_same_v<T, int> ||
            std::is_same_v<T, double> ||
            std::is_same_v<T, std::string> ||
            std::is_same_v<T, std::vector<int>> ||
            std::is_same_v<T, std::vector<double>>,
            "TaskContext::get_copy: unsupported type"
        );

        std::lock_guard<std::mutex> lock(mutex_);

        auto it = data_.find(key);
        if (it == data_.end()) {
            throw std::runtime_error("TaskContext: key not found: " + key);
        }

        const T* ptr = std::get_if<T>(it->second.get());
        if (!ptr) {
            throw std::runtime_error("TaskContext: bad type for key: " + key);
        }

        return *ptr;
    }

    template <class T>
    std::shared_ptr<const T> get_shared(const std::string& key) const {
        static_assert(
            std::is_same_v<T, int> ||
            std::is_same_v<T, double> ||
            std::is_same_v<T, std::string> ||
            std::is_same_v<T, std::vector<int>> ||
            std::is_same_v<T, std::vector<double>>,
            "TaskContext::get_shared: unsupported type"
        );

        std::shared_ptr<const Value> owner;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            auto it = data_.find(key);
            if (it == data_.end()) {
                throw std::runtime_error("TaskContext: key not found: " + key);
            }
            owner = it->second;
        }

        const T* ptr = std::get_if<T>(owner.get());
        if (!ptr) {
            throw std::runtime_error("TaskContext: bad type for key: " + key);
        }
        
        return std::shared_ptr<const T>(owner, ptr);
    }

    Value get_value_copy(const std::string& key) const {
        std::lock_guard<std::mutex> lock(mutex_);

        auto it = data_.find(key);
        if (it == data_.end()) {
            throw std::runtime_error("TaskContext: key not found: " + key);
        }

        return *it->second;
    }

    void set_value(const std::string& key, Value value) {
        auto stored = std::make_shared<const Value>(std::move(value));
        std::lock_guard<std::mutex> lock(mutex_);
        data_[key] = std::move(stored);
    }

    template <class T>
    void update(const std::string& key, T value) {
        set<T>(key, std::move(value));
    }

    bool contains(const std::string& key) const {
        std::lock_guard<std::mutex> lock(mutex_);
        return data_.find(key) != data_.end();
    }

    void erase(const std::string& key) {
        std::lock_guard<std::mutex> lock(mutex_);
        data_.erase(key);
    }

    void clear() {
        std::lock_guard<std::mutex> lock(mutex_);
        data_.clear();
    }
};